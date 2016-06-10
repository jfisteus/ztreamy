# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2015 Jesus Arias Fisteus
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
#
""" Implementation of streams and an asynchronous HTTP server for them.

Two kinds of streams are provided: 'Stream' and 'RelayStream'.

'Stream' is a basic stream of events. It can transmit events received
from remote sources or generated at the process of the server.

'RelayStream' extends the basic stream with functionality to listen to
other streams and retransmitting their events. Events from remote
sources or generated at the process of the server can also be
published in this type of stream.

The server is asynchronous. Be careful when doing blocking calls from
callbacks (for example, sources of events and filters), because the
server will be blocked.

"""
from __future__ import print_function

import logging
import traceback
import time
from datetime import timedelta
import re
import os.path
import inspect

import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httpserver

import ztreamy
from . import events
from .client import Client
from . import dispatchers
from .dispatchers import ClientProperties, ClientPropertiesFactory
from . import events_buffer
from .utils import parsing
from . import stats


# Uncomment to do memory profiling
#import guppy.heapy.RM


class StreamServer(tornado.web.Application):
    """An HTTP server for event streams.

    The server is able to serve one or more streams. Each stream is
    identified by its path in the server. A typical use of the server
    consists of creating it, registering one or more streams, and
    starting it::

    stream1 = Stream('/mystream1')
    stream2 = Stream('/mystream2')
    server = StreamServer(8080)
    server.add_stream(stream1)
    server.add_stream(stream2)
    server.start()

    """
    def __init__(self, port, ioloop=None, stop_when_source_finishes=False,
                 **kwargs):
        """Creates a new server.

        'port' specifies the port number in which the HTTP server will
        listen.

        'stop_when_source_finishes' set to True makes the server
        finish after the event source declares it has finished. Used
        for the performance experiments, but its value should normally
        be 'False'.

        The server won't start to serve streams until start() is
        called.  Streams cannot be registered in the server after it
        has been started.

        """
        super(StreamServer, self).__init__(**kwargs)
        logging.info('Initializing server...')
        self.http_server = tornado.httpserver.HTTPServer(self,
                                                decompress_request=True)
        self.streams = []
        self.port = port
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.stop_when_source_finishes = stop_when_source_finishes
        self._looping = False
        self._started = False
        self._stopped = False

    def add_stream(self, stream):
        """Adds a new stream to be served in this server.

        'stream' is an object of the class 'Stream', its sublcasses or
        any other class providing a similar interface.

        This method cannot be called after the server is started through
        its 'start()' method.

        """
        assert not self._started
        self.streams.append(stream)

    def start(self, loop=True):
        """Starts the server.

        The server begins to listen to HTTP requests.

        If 'loop' is true (which is the default), the server will
        block on the ioloop until 'close()' is called.

        """
        assert(not self._started)
        logging.info('Starting server...')
        self._register_handlers()
        self.http_server.listen(self.port)
        for stream in self.streams:
            stream.start()
        self._started = True
        if loop:
            self._looping = True
            self.ioloop.start()
            self._looping = False

    def stop(self):
        """Stops the server.

        If the server is blocked on the ioloop in the 'start()'
        method, it is released.

        The server object cannot be used again after been closed.

        """
        if self._started and not self._stopped:
            logging.info('Stopping server...')
            for stream in self.streams:
                stream.stop()
            self.http_server.stop()
            self._stopped = True
            if self._looping:
                self.ioloop.stop()
                self._looping = False

    def _register_handlers(self):
        handlers = []
        # Common static files
        static_path = os.path.join(os.path.dirname(__file__), 'data',
                                   'static')
        handlers.append(tornado.web.URLSpec(r'/static/(.*)',
                                            tornado.web.StaticFileHandler,
                                            kwargs=dict(path=static_path)))
        # Installed streams
        for stream in self.streams:
            handler_kwargs = {
                'stream': stream,
                'dispatcher': stream.dispatcher,
            }
            compressed_kwargs = {'force_compression': True}
            compressed_kwargs.update(handler_kwargs)
            priority_kwargs = {'force_compression': False, 'priority': True}
            priority_kwargs.update(handler_kwargs)
            handlers.extend([
                ## tornado.web.URLSpec(r"/", _MainHandler),
                tornado.web.URLSpec(stream.path + r"/stream",
                                    _EventStreamHandler,
                                    kwargs=handler_kwargs),
                tornado.web.URLSpec(stream.path + r"/compressed",
                                    _EventStreamHandler,
                                    kwargs=compressed_kwargs),
                tornado.web.URLSpec(stream.path + r"/priority",
                                    _EventStreamHandler,
                                    kwargs=priority_kwargs),
                tornado.web.URLSpec(stream.path + r"/long-polling",
                                    _ShortLivedHandler,
                                    kwargs=handler_kwargs),
                tornado.web.URLSpec(stream.path + r"/(dashboard.html)",
                                    tornado.web.StaticFileHandler,
                                    kwargs=dict(path=static_path)),
            ])
            if stream.allow_publish:
                publish_kwargs = {'stream': stream,
                                  'stop_when_source_finishes': \
                                       self.stop_when_source_finishes}
                handlers.append(tornado.web.URLSpec(stream.path + r"/publish",
                                                    stream.publish_handler,
                                                    kwargs=publish_kwargs))
                handlers.append(tornado.web.URLSpec( \
                                            stream.path + r"/publish-cont",
                                            ContinuousPublishHandler,
                                            kwargs=publish_kwargs))
        self.add_handlers(".*$", handlers)


class Stream(object):
    """Stream of events.

    The stream must be published through a web server::

    server = StreamServer(8080)
    stream = Stream('/mystream')
    server.add_stream(stream)

    The stream has an associated path prefix, which is used to
    identify the stream within the server (a server can serve more
    than one stream). Clients connect to the server and provide the
    appropriate HTTP path. This path is the concatenation of the
    prefix of the stream and the following subcomponents:

    '/': not implemented yet. Intended for human readable
        information about the stream.

    '/stream': stream with long-lived response. The
        response is not closed by the server, and events are sent to
        the client as they are available. A client library able to
        notify the user as new chunks of data are received is needed
        in order to follow this stream.
        Compression depends on the Accept-Encoding header sent by
        the client. The 'deflate' choice should be specified
        there with higher priority than 'identity'.

    '/compressed': zlib-compressed stream with long-lived response.
        Similar to how the previous path works.
        The difference is that 'deflate' compression is always
        used regardless the Accept-Encoding header.

    '/priority': uncompressed stream with lower delays intended for
        clients with priority, typically relay servers.
        The Accept-Encoding header is ignored.

    '/long-polling': available events are sent to the client uncompressed.
        The request is closed immediately. The client can specify the
        latest event it has received in order to get the events that
        follow it.

    '/publish': receives events from event sources in order to be
        published in the stream.

    There are two ways of publishing events in a stream: sending them
    through HTTP using the .../publish path, if allowed by the
    configuration of the stream, or using locally its
    'dispatch_event()' or 'dispatch_events()' methods.

    """
    def __init__(self, path,
                 label=None,
                 source_id=None,
                 allow_publish=False,
                 buffering_time=None,
                 num_recent_events=2048,
                 persist_events=False,
                 event_adapter=None,
                 parse_event_body=True,
                 custom_publish_handler=None,
                 ioloop=None):
        """Creates a stream object.

        The stream will be served with the specified 'path' prefix,
        which should start with a slash ('/') and should not end with
        one. If 'path' does not start with a slash, this constructor
        inserts it automatically. For example, if the value of 'path'
        is '/mystream' the stream will be served as:

        http://host:port/mystream/stream
        http://host:port/mystream/compressed
        (etc.)

        The stream does not accept events from clients, unless
        'allow_publish' is set to 'True'.

        'buffering_time' controls the period for which events are
        accumulated in a buffer before sending them to the
        clients. Higher times improve CPU performance in the server
        and compression ratios, but increase the latency in the
        delivery of events.

        If 'custom_publish_handler' is None, the default publish handler
        is used. When a custom handler is necessary pass the class of
        the handler as an argument.

        If a 'ioloop' object is given, it will be used by the internal
        timers of the stream.  If not, the default 'ioloop' of the
        Tornado instance will be used.

        """
        if persist_events and label is None:
            raise ValueError('Persisting recent events requires '
                             'a stream label')
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.label = label
        if source_id is not None:
            self.source_id = source_id
        else:
            self.source_id = ztreamy.random_id()
        if path.startswith('/'):
            self.path = path
        else:
            self.path = '/' + path
        self.allow_publish = allow_publish
        if not persist_events:
            self.event_buffer = events_buffer.PendingEventsBuffer()
        else:
            self.event_buffer = \
                events_buffer.PendingEventsBufferAsync(self.label, self.ioloop)
        self.dispatcher = _EventDispatcher( \
                                self,
                                num_recent_events=num_recent_events,
                                persist_events=persist_events)
        self.buffering_time = buffering_time
        self.event_adapter = event_adapter
        if custom_publish_handler is None:
            self.publish_handler = EventPublishHandler
        else:
            if (inspect.isclass(custom_publish_handler)
                and issubclass(custom_publish_handler,
                               tornado.web.RequestHandler)):
                self.publish_handler = custom_publish_handler
            else:
                 raise ValueError('custom_publish_handler expected to be '
                                  'a class (tornado.web.RequestHandler '
                                  'or subclass')
        self.parse_event_body = parse_event_body
        if buffering_time:
            self.buffer_dump_sched = \
                tornado.ioloop.PeriodicCallback(self._dump_buffer,
                                                buffering_time, self.ioloop)
        self.stats = stats.StreamStats()
        self.running = False
        ## self.stats_sched = tornado.ioloop.PeriodicCallback( \
        ##                                   self.dispatcher.stats, 10000,
        ##                                   self.ioloop)

    def start(self):
        """Starts the stream.

        The StreamServer will automatically call this method when it
        is started. User code won't probably need to call it.

        """
        if self.buffering_time:
            self.buffer_dump_sched.start()
        self.running = True
        ## self.stats_sched.start()

    def stop(self):
        """Stops the stream.

        The StreamServer will automatically call this method when it
        is stopped. User code won't probably need to call it.

        """
        ## self.dispatch_event(events.create_command(self.source_id,
        ##                                           'Stream-Finished'))
        if self.buffering_time:
            self.buffer_dump_sched.stop()
            self._dump_buffer()
        self.dispatcher.close()
        self.running = False
        ## self.stats_sched.stop()

    def dispatch_event(self, event):
        """Publishes an event in this stream.

        The event may not be sent immediately to normal clients if the
        server is configured to do buffering. However, it will be sent
        immediately to priority clients.

        """
        self.dispatch_events([event])

    def dispatch_events(self, evs):
        """Publishes a list of events in this stream.

        The events may not be sent immediately to normal clients if
        the stream is configured to do buffering. However, they will
        be sent immediately to priority clients.

        """
        accepted_events = []
        for e in evs:
            if (not self.event_buffer.is_duplicate(e)
                and not self.dispatcher.is_duplicate(e)):
                e.aggregator_id.append(self.source_id)
                accepted_events.append(e)
        if self.event_adapter:
            accepted_events = self.event_adapter(accepted_events)
        self.dispatcher.dispatch_immediate(accepted_events)
        if self.buffering_time is None:
            self.dispatcher.dispatch(accepted_events)
        else:
            self.event_buffer.add_events(accepted_events)
        return(accepted_events)

    def create_local_client(self, callback, separate_events=True):
        """Creates a local client for this stream.

        'callback' is a callback function to be called when new events
        are available in the stream. If 'separate_events' is True (the
        default), the callback receives just one event object in each
        call. It it is 'false', the callback receives a list which may
        contain one or more event objects.

        """
        properties = dispatchers.ClientPropertiesFactory.create_local_client()
        client = _LocalClient(callback, properties,
                              separate_events=separate_events)
        self.dispatcher.register_client(client)
        return client

    def preload_recent_events_buffer_from_file(self, file_):
        self.dispatcher.recent_events.load_from_file(file_)

    def _dump_buffer(self):
        self.dispatcher.dispatch(self.event_buffer.get_events())

    def _finish_when_possible(self):
        self.dispatcher._auto_finish = True
        if self.buffering_time is None or self.buffering_time <= 0:
            self.ioloop.add_timeout(timedelta(seconds=20), self._finish)

    def _finish(self):
        self.dispatcher.close()
        self.ioloop.stop()


class RelayStream(Stream):
    """A stream that relays events from other streams.

    This stream listens to other event streams and publishes their
    events as a new stream. A filter can optionally be applied in
    order to select which events are published.

    """
    def __init__(self, path, streams,
                 label=None,
                 filter_=None,
                 allow_publish=False,
                 buffering_time=None,
                 num_recent_events=2048,
                 persist_events=False,
                 event_adapter=None,
                 parse_event_body=False,
                 retrieve_missing_events=False,
                 ioloop=None,
                 stop_when_source_finishes=False):
        """Creates a new relay stream.

        The stream will retransmit the events of the streams specified
        in 'streams' (either a list of streams or a single stream).
        Each stream can be either a string representing the stream URL
        or a local 'server.Stream' (or compatible) object.  A filter
        may be specified in 'filter_' in order to select which events
        are relayed (see the 'filters' module for more information).

        An 'event_adapter' is a function that receives a list of
        events, adapts them and returns a list of adapted events,
        which are the ones that will be relayed.

        A 'label' (string) may be set to the clients of this relay.
        Setting a label allows each client to save the id of the
        latest event it received and ask for missed events
        when the client is run again. Set 'retrieve_missing_events'
        to True in order to do that.
        If 'retrieve_missing_events' is True, a non-empty label must be set.

        The rest of the parameters are as described in the constructor
        of the Stream class.

        """
        super(RelayStream, self).__init__( \
                                path,
                                label=label,
                                allow_publish=allow_publish,
                                buffering_time=buffering_time,
                                num_recent_events=num_recent_events,
                                persist_events=persist_events,
                                event_adapter=event_adapter,
                                parse_event_body=parse_event_body,
                                ioloop=ioloop)
        if filter_ is not None:
            filter_.callback = self._relay_events
            event_callback = filter_.filter_events
        else:
            event_callback = self._relay_events
        self.stop_when_source_finishes = stop_when_source_finishes
        self.client = Client(streams, event_callback,
                        error_callback=self._handle_error,
                        source_finish_callback=self._handle_source_finish,
                        parse_event_body=parse_event_body,
                        separate_events=False,
                        label=label,
                        retrieve_missing_events=retrieve_missing_events,
                        ioloop=ioloop)

    def start(self):
        """Starts the relay stream.

        The StreamServer will automatically call this method when it
        is started. User code won't probably need to call it.

        """
        self.client.start(loop=False)
        super(RelayStream, self).start()

    def stop(self):
        """Stops the relay stream.

        The StreamServer will automatically call this method when it
        is stopped. User code won't probably need to call it.

        """
        self.client.stop()
        super(RelayStream, self).stop()

    def _relay_events(self, evs):
        if isinstance(evs, events.Event):
            evs = [evs]
        self.dispatch_events(evs)

    def _handle_error(self, message, http_error=None):
        if http_error is not None:
            logging.error(message + ': ' + str(http_error))
        else:
            logging.error(message)

    def _handle_source_finish(self):
        if self.stop_when_source_finishes:
            self._finish_when_possible()


class _Client(object):
    def __init__(self, handler, callback, properties):
        self.handler = handler
        self.callback = callback
        self.properties = properties
        self.closed = False
        self.creation_time = time.time()
        self.is_fresh = True

    def send(self, data, flush=True):
        self.callback(data, flush=flush)
        if data:
            self.is_fresh = False

    def send_initial_events(self, evs):
        serialized = ztreamy.serialize_events( \
                                evs,
                                serialization=self.properties.serialization)
        if self.properties.encoding == ClientProperties.ENCODING_PLAIN:
            data = serialized
        elif self.properties.encoding == ClientProperties.ENCODING_ZLIB:
            data = dispatchers.compress_zlib(serialized)
        elif self.properties.encoding == ClientProperties.ENCODING_GZIP:
            data = dispatchers.compress_gzip(serialized)
        self.send(data)

    def close(self):
        """Closes the connection to this client.

        It does nothing if the connection is already closed.

        """
        self.closed = True
        if not self.handler.request.connection.stream.closed():
            ## logging.info('Finishing a client...')
            self.handler.finish()


class _LocalClient(object):
    """Handle for a local client.

    Do not create instances of this class directly. An instance will
    be returned by calling 'create_local_client()' in 'Stream'.  In
    order to disconnect from the stream, call 'close()' on this
    object.

    """
    def __init__(self, callback, properties, separate_events=True):
        """Not intended to be called by the user.

        Use 'create_local_client()' in 'Stream' instead.

        """
        if not properties.local:
            raise ValueError('Non-local properties for local client')
        self.callback = callback
        self.separate_events = separate_events
        self.properties = properties
        self.closed = False
        self.creation_time = time.time()

    def close(self):
        """Disconnects from the stream."""
        self.closed = True

    def send_events(self, evs):
        if not self.separate_events:
            self.callback(evs)
        else:
            for event in evs:
                self.callback(event)


class _EventDispatcher(object):
    def __init__(self, stream, num_recent_events=2048,
                 persist_events=False, ioloop=None):
        self.stream = stream
        self.dispatchers = {}
        self.immediate_dispatchers = []
        self.buffered_dispatchers = []
        self._init_dispatchers()
        self.last_event_time = time.time()
        self._auto_finish = False
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        if not persist_events:
            self.recent_events = \
                events_buffer.EventsBuffer(num_recent_events)
        else:
            self.recent_events = \
                events_buffer.EventsBufferAsync(num_recent_events,
                                                stream.label,
                                                stream.event_buffer)
        self.periodic_maintenance_timer = tornado.ioloop.PeriodicCallback( \
                                                   self._periodic_maintenance,
                                                   60000,
                                                   io_loop=self.ioloop)
        self.periodic_maintenance_timer.start()

    @property
    def num_clients(self):
        return sum(len(dispatcher) for dispatcher in self.dispatchers.values())

    def register_client(self, client, last_event_seen=None,
                        past_events_limit=None, non_blocking=False):
        dispatcher = self.dispatchers.get(client.properties, None)
        if dispatcher is None:
            raise ValueError('Not appropriate dispatcher')
        past_data = []
        if last_event_seen:
            # Send the available events after the last seen event
            past_data, none_lost = self.recent_events.newer_than( \
                                                    last_event_seen,
                                                    limit=past_events_limit)
        elif past_events_limit is not None:
            past_data = self.recent_events.most_recent(past_events_limit)
        if past_data or non_blocking:
            client.send_initial_events(past_data)
            if not client.properties.streaming:
                client.close()
        if not client.closed:
            dispatcher.subscribe(client)
            client.dispatcher = dispatcher

    def deregister_client(self, client):
        client.dispatcher.unsubscribe(client)
        client.close()

    def dispatch_immediate(self, evs):
        pack = dispatchers.EventsPack(evs)
        for dispatcher in self.immediate_dispatchers:
            dispatcher.dispatch(pack)

    def dispatch(self, evs):
        logging.debug('{}: server cycle; events: {}'.format(self.stream.path,
                                                            len(evs)))
        self.recent_events.append_events(evs)
        if not evs:
            if self._auto_finish and time.time() - self.last_event_time > 60:
                self.close()
                self.ioloop.stop()
        else:
            self.last_event_time = time.time()
        pack = dispatchers.EventsPack(evs)
        for dispatcher in self.buffered_dispatchers:
            dispatcher.dispatch(pack)
            if evs and not dispatcher.properties.streaming:
                dispatcher.close()
        self.stream.stats.count_events(len(evs))

    def close(self):
        """Closes every active streaming client."""
        for dispatcher in self.dispatchers.values():
            dispatcher.close()

    def is_duplicate(self, event):
        """Checks if an event with the same id has already been dispatched."""
        return self.recent_events.contains(event)

    def _periodic_maintenance(self):
        for dispatcher in self.dispatchers.values():
            dispatcher.periodic_maintenance()

    def _init_dispatchers(self):
        # Streaming dispatcher, plain encoding, ztreamy serialization:
        properties = ClientPropertiesFactory.create( \
                                streaming=True)
        dispatcher = dispatchers.SimpleDispatcher(self.stream, properties)
        self.dispatchers[properties] = dispatcher
        self.buffered_dispatchers.append(dispatcher)
        # Streaming dispatcher, zlib encoding, ztreamy serialization:
        properties = ClientPropertiesFactory.create( \
                                streaming=True,
                                encoding=ClientProperties.ENCODING_ZLIB)
        dispatcher = dispatchers.ZlibDispatcher(self.stream, properties)
        self.dispatchers[properties] = dispatcher
        self.buffered_dispatchers.append(dispatcher)
        # Streaming dispatcher, pain encoding, json serialization:
        properties = ClientPropertiesFactory.create( \
                                streaming=True,
                                serialization=ztreamy.SERIALIZATION_LDJSON)
        dispatcher = dispatchers.SimpleDispatcher(self.stream, properties)
        self.dispatchers[properties] = dispatcher
        self.buffered_dispatchers.append(dispatcher)
        # Streaming dispatcher, zlib encoding, json serialization:
        properties = ClientPropertiesFactory.create( \
                                streaming=True,
                                serialization=ztreamy.SERIALIZATION_LDJSON,
                                encoding=ClientProperties.ENCODING_ZLIB)
        dispatcher = dispatchers.ZlibDispatcher(self.stream, properties)
        self.dispatchers[properties] = dispatcher
        self.buffered_dispatchers.append(dispatcher)
        # Long polling dispatcher, plain encoding, ztreamy serialization:
        properties = ClientPropertiesFactory.create()
        dispatcher = dispatchers.SimpleDispatcher(self.stream, properties)
        self.dispatchers[properties] = dispatcher
        self.buffered_dispatchers.append(dispatcher)
        # Long polling dispatcher, plain encoding, json serialization:
        properties = ClientPropertiesFactory.create( \
                                serialization=ztreamy.SERIALIZATION_JSON)
        dispatcher = dispatchers.SimpleDispatcher(self.stream, properties)
        self.dispatchers[properties] = dispatcher
        self.buffered_dispatchers.append(dispatcher)
        # Long polling dispatcher, gzip encoding, ztreamy serialization:
        properties = ClientPropertiesFactory.create( \
                                encoding=ClientProperties.ENCODING_GZIP)
        dispatcher = dispatchers.SimpleDispatcher(self.stream, properties)
        self.dispatchers[properties] = dispatcher
        self.buffered_dispatchers.append(dispatcher)
        # Long polling dispatcher, gzip encoding, json serialization:
        properties = ClientPropertiesFactory.create( \
                                serialization=ztreamy.SERIALIZATION_JSON,
                                encoding=ClientProperties.ENCODING_GZIP)
        dispatcher = dispatchers.SimpleDispatcher(self.stream, properties)
        self.dispatchers[properties] = dispatcher
        self.buffered_dispatchers.append(dispatcher)
        # Priority dispatcher
        properties = ClientPropertiesFactory.create( \
                                streaming=True,
                                priority=True)
        dispatcher = dispatchers.SimpleDispatcher(self.stream, properties)
        self.dispatchers[properties] = dispatcher
        self.immediate_dispatchers.append(dispatcher)
        # Local dispatcher
        properties = ClientPropertiesFactory.create_local_client()
        dispatcher = dispatchers.LocalDispatcher(self.stream, properties)
        self.dispatchers[properties] = dispatcher
        self.immediate_dispatchers.append(dispatcher)


class GenericHandler(tornado.web.RequestHandler):
    _q_re = re.compile(r'^\s*(\w+)\s*;\s*q=(0(\.[0-9]{1,3})?|1(\.0{1,3})?)$')
    _attribute_re = re.compile(r'^\s*(\w+)\s*')

    def __init__(self, application, request, **kwargs):
        super(GenericHandler, self).__init__(application, request, **kwargs)
        self.application = application

    @property
    def ioloop(self):
        return self.application.ioloop

    def req_content_type(self):
        return parsing.get_content_type(self.request.headers['Content-Type'])

    def _last_seen_parameters(self):
        last_event_seen = self.get_argument('last-seen', default=None)
        past_events_limit = self.get_argument('past-events-limit',
                                              default=None)
        if past_events_limit == '':
            past_events_limit = None
        elif past_events_limit is not None:
            try:
                past_events_limit = int(past_events_limit)
            except ValueError:
                raise tornado.web.HTTPError(404, 'Not Found')
            else:
                if past_events_limit < 0:
                    raise tornado.web.HTTPError(404, 'Not Found')
        non_blocking = self.get_argument('non-blocking', default=None)
        if non_blocking is not None and non_blocking != '0':
            non_blocking = True
        else:
            non_blocking = False
        return last_event_seen, past_events_limit, non_blocking

    def _select_encoding(self, acceptable_encodings):
        """Selects an appropriate encoding for the HTTP response.

        It receives the list of encodings the server accepts
        for this request, analyzes the Accept-Encoding header
        of the request and finds the best match.

        Returns None if no suitable encoding is found.

        """
        if not acceptable_encodings:
            raise ValueError('Empty acceptable encodings list')
        encodings = self._accept_values('Accept-Encoding')
        if not encodings:
            encodings = ['identity']
        for encoding in encodings:
            if encoding in acceptable_encodings:
                selected = encoding
                break
            elif encoding == '*':
                selected = acceptable_encodings[0]
                break
        else:
            # No suitable encoding found
            selected = None
        return selected

    def _accept_values(self, header_name):
        """Returns the list of the items of a comma-separated header value.

        Quality values are processed according to HTTP content negotiation.

        """
        return GenericHandler._accept_values_internal( \
                                    self.request.headers.get_list(header_name))

    @staticmethod
    def _accept_values_internal(header_value_list):
        """Returns the list of the items of a comma-separated header value.

        Quality values are processed according to HTTP content negotiation.

        """
        values = []
        for value_list in header_value_list:
            values.extend([value.strip() for value in value_list.split(',')])
        q_values = []
        for i, value in enumerate(values):
            attribute, quality = GenericHandler._read_q_value(value)
            q_values.append((-quality, i, attribute))
        return [v[2] for v in sorted(q_values)]

    @staticmethod
    def _read_q_value(value):
        correct = False
        if ';' in value:
            r = GenericHandler._q_re.search(value)
            if r:
                attribute = r.group(1)
                try:
                    quality = float(r.group(2))
                    correct = True
                except ValueError:
                    # correct remains False
                    pass
        else:
            r = GenericHandler._attribute_re.search(value)
            if r:
                attribute = r.group(1)
                quality = 1.0
                correct = True
        if  correct:
            return attribute, quality
        else:
            raise tornado.web.HTTPError(400, 'Bad Request')


class _MainHandler(GenericHandler):
    def get(self):
        raise tornado.web.HTTPError(404)


class EventPublishHandler(GenericHandler):
    """ Synchronous event publishing handler.

    Extend this class if you need to send a custom response to the source
    of the events (override methods get/post so that they fit your needs).

    If your response depends on external services (e.g. you need to do
    an HTTP request before answering the client, extend instead
    `EventPublishHandlerAsync`, which is the asynchronous version
    of this handler.

    """

    def __init__(self, application, request, stream=None,
                 stop_when_source_finishes=False):
        super(EventPublishHandler, self).__init__(application, request)
        self.stream = stream
        self.stop_when_source_finishes = stop_when_source_finishes
        if stream is None:
            raise ValueError('EventPublishHandler requires a stream object')

    def get(self):
        if self.stream.running:
            self.get_and_dispatch_events()
        else:
            raise tornado.web.HTTPError(503, 'The stream is stopped')

    def post(self):
        if self.stream.running:
            self.get_and_dispatch_events()
        else:
            raise tornado.web.HTTPError(503, 'The stream is stopped')

    def retrieve_events(self):
        """ Gets the events from a GET or POST message.

        Returns a list of events.Event objects.

        Exceptions may be risen due to errors in the request.

        """
        if self.request.method == 'GET':
            evs = [self.retrieve_event_get()]
        elif self.request.method == 'POST':
            evs = self.retrieve_events_post()
        else:
            raise ValueError('Cannot retrieve event from method {}',
                             self.request.method)
        return evs

    def retrieve_events_checked(self):
        """ Gets the events from a GET or POST message.

        Returns a list of events.Event objects.

        If exceptions happens, they are caught and an HTTPError is re-raised.

        """
        try:
            evs = self.retrieve_events()
        except Exception as ex:
            traceback.print_exc()
            raise tornado.web.HTTPError(400, str(ex))
        return evs

    def retrieve_event_get(self):
        """ Gets the event data for a single event from a GET request.

        Returns one events.Event object.

        The HTTP GET request may have the following request properties:
            - 'event-id': the event id. If not present, a random one
                is created.
            - 'source-id' (required): the source id of the event.
            - 'syntax' (required): the syntax of the body of the event.
            - 'body' (required): the body of the event.
            - 'application-id': application id of the event.
            - 'aggregator-ids': the aggregator ids of the event
                (a comma-sparated list of ids).
            - 'event-type': the event type of the event.
            - 'timestamp': the timestamp of the event.
        """
        event_id = self.get_argument('event-id', default=None)
        if event_id is None:
            event_id = ztreamy.random_id()
        source_id = self.get_argument('source-id')
        syntax = self.get_argument('syntax')
        body = self.get_argument('body')
        application_id = self.get_argument('application-id', default=None)
        aggregator_id = events.parse_aggregator_id( \
            self.get_argument('aggregator-ids', default=''))
        event_type = self.get_argument('event-type', default=None)
        timestamp = self.get_argument('timestamp', default=None)
        event = events.Event(source_id, syntax, body,
                             application_id=application_id,
                             aggregator_id=aggregator_id,
                             event_type=event_type, timestamp=timestamp)
        return event

    def retrieve_events_post(self):
        """ Gets a list of events from a POST request.

        The method reads the Content-Type header in order to parse
        the events as ztreamy.event_media_type or ztreamy.json_media_type.

        The method does not catch any parsing exception that might happen.

        """
        try:
            content_type = self.req_content_type()
        except ValueError:
            raise tornado.web.HTTPError(400, 'Bad content type')
        if content_type == ztreamy.event_media_type:
            deserializer = events.Deserializer()
            parse_body = self.stream.parse_event_body
        elif content_type == ztreamy.json_media_type:
            deserializer = events.JSONDeserializer()
            parse_body = True
        else:
            raise tornado.web.HTTPError(400, 'Bad content type')
        return deserializer.deserialize(self.request.body,
                                        parse_body=parse_body,
                                        complete=True)

    def process_events(self, evs):
        """ Does the event processing and dispatching tasks."""
        for event in evs:
            if event.syntax == 'ztreamy-command':
                if event.command == 'Event-Source-Finished':
                    if self.stop_when_source_finishes:
                        self.stream._finish_when_possible()
        return self.stream.dispatch_events(evs)

    def get_and_dispatch_events(self, finish_request=True):
        """ Publishes the events.

        This methods includes all the tasks needed to get the events
        from the request, process them and dispatch them.

        Returns the list of events that were dispatched.

        If `finish_request` is True (the default), the request is finished
        with an empty 200 OK message.

        """
        evs = self.retrieve_events_checked()
        accepted_events = self.process_events(evs)
        if finish_request:
            self.finish()
        return accepted_events


class EventPublishHandlerAsync(EventPublishHandler):
    """ Asynchronous event publishing handler.

    Extend this class if you need to send a custom response to the client
    that sends the event in an asynchronous way (e.g. you need to
    do an asynchronous HTTP request before answering the client).

    Override get/post as needed, but remember that you are responsible
    in this case of getting the events
    (events = self.retrieve_events_checked()) and
    dispatching them (self.process_events(events)).
    The method self.get_and_dispatch_events(finish_request=False)
    does that and leaves the response unanswered for further processing
    from your custom code.

    You can also set a timeout so that a response is sent if your
    asynchronous calls do not finish on time.

    """
    def __init__(self, application, request, **kwargs):
        super(EventPublishHandlerAsync, self).__init__(application, request,
                                                       **kwargs)
        self.finished = False

    @tornado.web.asynchronous
    def get(self):
        if self.stream.running:
            self.get_and_dispatch_events(finish_request=True)
        else:
            raise tornado.web.HTTPError(503, 'The stream is stopped')

    @tornado.web.asynchronous
    def post(self):
        if self.stream.running:
            self.get_and_dispatch_events(finish_request=True)
        else:
            raise tornado.web.HTTPError(503, 'The stream is stopped')

    def set_response_timeout(self, timeout):
        """ Sets a response timeout.

        If the response is not finished before the timeout,
        the method `self.on_response_timeout` is called
        in order to close it.

        The `timeout` is given as a (possibly float) number of seconds.

        """
        self.ioloop.call_later(timeout, self.on_response_timeout)

    def on_response_timeout(self):
        """ Called when the response timeout fires.

        This method closes the response with a 200 OK answer and no body.
        It can be overriden by subclasses for custom responses,
        but it should close the response always.

        This callback might be called even after the response has already
        been finished, since this class does not cancel the timeout
        for simplicity. Use the `self.finished` atribute if you need
        to check that the response hasn't already been finished.
        Note, however, that calling the self.finish() method more than
        once is safe since it already checks that attribute.

        """
        if not self.finished:
            logging.debug('Timeout a source request')
        self.finish()

    def finish(self, *args, **kwargs):
        if not self.finished:
            self.finished = True
            super(EventPublishHandlerAsync, self).finish(*args, **kwargs)


@tornado.web.stream_request_body
class ContinuousPublishHandler(GenericHandler):
    """ Asynchronous event publishing handler for continuous requests.

    This handler allows clients to upload an unbounded continuous
    stream of data with just one HTTP POST request.

    Clients should use the Ztreamy serialization `ztreamy.stream_media_type`
    or the line-delimited JSON serialization `ztreamy.ldjson_media_type`.

    See the class `ztreamy.client.ContinuousEventPublisher` for an
    example client implementation for this handler.

    """
    def __init__(self, application, request, stream=None,
                 stop_when_source_finishes=False):
        super(ContinuousPublishHandler, self).__init__(application, request)
        self.stream = stream
        self.deserializer = None
        self.parse_body = self.stream.parse_event_body
        if stream is None:
            raise ValueError('ContinuousPublishHandler requires '
                             'a stream object')

    def prepare(self):
        """Check the method and content type of the request.

        Called once after the headers are parsed, but before the first
        call to `data_received`.

        """
        if self.request.method != 'POST':
            raise tornado.web.HTTPError(405, 'Method not allowed')
        try:
            content_type = self.req_content_type()
        except ValueError:
            raise tornado.web.HTTPError(400, 'Bad content type')
        if content_type == ztreamy.stream_media_type:
            self.deserializer = events.Deserializer()
        elif content_type == ztreamy.ldjson_media_type:
            self.deserializer = events.LDJSONDeserializer()
            self.parse_body = True
        else:
            raise tornado.web.HTTPError(400, 'Bad content type')

    def data_received(self, data):
        try:
            evs = self.deserializer.deserialize(data, complete=False,
                                                parse_body=self.parse_body)
        except (ValueError, ztreamy.ZtreamyException):
            raise tornado.web.HTTPError(400, 'Bad request')
        self.stream.dispatch_events(evs)

    def post(self):
        """ Called when the request gets finished."""
        if self.deserializer.data_is_pending():
            raise tornado.web.HTTPError(400, 'Bad request')


class _EventStreamHandler(GenericHandler):
    def __init__(self, application, request, stream=None,
                 dispatcher=None, force_compression=False, priority=False):
        super(_EventStreamHandler, self).__init__(application, request)
        self.dispatcher = dispatcher
        self.force_compression = force_compression
        self.stream = stream
        self.priority = priority

    @tornado.web.asynchronous
    def get(self):
        # non_blocking will be ignored and False used always!
        last_event_seen, past_events_limit, non_blocking = \
            self._last_seen_parameters()
        if ('Accept' in self.request.headers
            and ztreamy.ldjson_media_type in self.request.headers['Accept']):
            serialization = ztreamy.SERIALIZATION_LDJSON
        else:
            serialization = ztreamy.SERIALIZATION_ZTREAMY
        if not self.priority:
            if self.force_compression:
                encoding = 'deflate'
            else:
                encoding = self._select_encoding(['deflate', 'identity'])
        else:
            encoding = 'identity'
        if encoding is not None:
            if encoding == 'deflate':
                encoding = ClientProperties.ENCODING_ZLIB
            else:
                encoding = ClientProperties.ENCODING_PLAIN
            if serialization == ztreamy.SERIALIZATION_ZTREAMY:
                self.set_header('Content-Type', ztreamy.stream_media_type)
            else:
                self.set_header('Content-Type', ztreamy.ldjson_media_type)
            # Allow cross-origin with CORS (see http://www.w3.org/TR/cors/):
            self.set_header('Access-Control-Allow-Origin', '*')
            if encoding == ClientProperties.ENCODING_ZLIB:
                self.set_header('Content-Encoding', 'deflate')
            properties = ClientPropertiesFactory.create( \
                                streaming=True,
                                serialization=serialization,
                                encoding=encoding,
                                priority=self.priority)
            self.client = _Client(self, self._on_new_data, properties)
            self.dispatcher.register_client( \
                                        self.client,
                                        last_event_seen=last_event_seen,
                                        past_events_limit=past_events_limit,
                                        non_blocking=False)
        else:
            raise tornado.web.HTTPError(406, 'Not Acceptable')

    def _on_new_data(self, data, flush=True):
        if self.request.connection.stream.closed():
            self.dispatcher.deregister_client(self.client)
            return
        self.write(data)
        if flush:
            self.flush()


class _ShortLivedHandler(GenericHandler):
    def __init__(self, application, request, dispatcher=None, stream=None):
        super(_ShortLivedHandler, self).__init__(application, request)
        self.dispatcher = dispatcher
        # the 'stream' argument is ignored (not necessary)

    @tornado.web.asynchronous
    def get(self):
        last_event_seen, past_events_limit, non_blocking = \
            self._last_seen_parameters()
        if ('Accept' in self.request.headers
            and ztreamy.json_media_type in self.request.headers['Accept']):
            serialization = ztreamy.SERIALIZATION_JSON
            self.set_header('Content-Type', ztreamy.json_media_type)
        else:
            serialization=ztreamy.SERIALIZATION_ZTREAMY
            self.set_header('Content-Type', ztreamy.stream_media_type)
        self.set_header('Access-Control-Allow-Origin', '*')
        encoding = self._select_encoding(['gzip', 'identity'])
        if encoding == 'gzip':
            encoding = ClientProperties.ENCODING_GZIP
            self.set_header('Content-Encoding', 'gzip')
        else:
            encoding = ClientProperties.ENCODING_PLAIN
        properties = ClientPropertiesFactory.create( \
                                streaming=False,
                                serialization=serialization,
                                encoding=encoding)
        self.client = _Client(self, self._on_new_data, properties)
        self.dispatcher.register_client(self.client,
                                        last_event_seen=last_event_seen,
                                        past_events_limit=past_events_limit,
                                        non_blocking=non_blocking)

    def _on_new_data(self, data, flush=False):
        # No need to flush because the request will be soon completed
        # The parameter is kept for compatibility
        if not self.request.connection.stream.closed():
            self.write(data)


def main():
    import time
    import tornado.options
    from ztreamy import rdfevents
    source_id = ztreamy.random_id()
    application_id = '1111-1111'

    def stop_server():
        server.stop()

    tornado.options.define('port', default=8888, help='run on the given port',
                           type=int)
    tornado.options.define('buffer', default=None, help='event buffer time (s)',
                           type=float)
    tornado.options.define('autostop', default=False,
                           help='stop the server when the source finishes',
                           type=bool)
    tornado.options.parse_command_line()
    port = tornado.options.options.port
    if (tornado.options.options.buffer is not None
        and tornado.options.options.buffer > 0):
        buffering_time = tornado.options.options.buffer * 1000
    else:
        buffering_time = None
    server = StreamServer(port,
                 stop_when_source_finishes=tornado.options.options.autostop)
    stream = Stream('/events', allow_publish=True,
                    buffering_time=buffering_time)
    ## relay = RelayStream('/relay', [('http://localhost:' + str(port)
    ##                                + '/stream/priority')],
    ##                     allow_publish=True,
    ##                     buffering_time=buffering_time)
    server.add_stream(stream)
    ## server.add_stream(relay)
     # Uncomment to test Stream.stop():
#    tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 5, stop_server)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()


if __name__ == "__main__":
    main()
