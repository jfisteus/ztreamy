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
import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httpserver
import traceback
import time
from datetime import timedelta
import re
import os.path

import ztreamy
from ztreamy import events, logger
from .client import Client
from . import dispatchers
from .dispatchers import ClientProperties, ClientPropertiesFactory

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
    def __init__(self, port, ioloop=None, stop_when_source_finishes=False):
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
        settings = dict()
        super(StreamServer, self).__init__(**settings)
        logging.info('Initializing server...')
        self.http_server = tornado.httpserver.HTTPServer(self)
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
            if self._looping == True:
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
                                                    _EventPublishHandler,
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
    def __init__(self, path, allow_publish=False, buffering_time=None,
                 source_id=None, num_recent_events=2048,
                 event_adapter=None,
                 parse_event_body=True, ioloop=None):
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

        If a 'ioloop' object is given, it will be used by the internal
        timers of the stream.  If not, the default 'ioloop' of the
        Tornado instance will be used.

        """
        if source_id is not None:
            self.source_id = source_id
        else:
            self.source_id = ztreamy.random_id()
        if path.startswith('/'):
            self.path = path
        else:
            self.path = '/' + path
        self.allow_publish = allow_publish
        self.dispatcher = _EventDispatcher(self,
                                           num_recent_events=num_recent_events)
        self.buffering_time = buffering_time
        self.event_adapter = event_adapter
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.parse_event_body = parse_event_body
        self._event_buffer = []
        if buffering_time:
            self.buffer_dump_sched = \
                tornado.ioloop.PeriodicCallback(self._dump_buffer,
                                                buffering_time, self.ioloop)
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
        ## self.stats_sched.start()

    def stop(self):
        """Stops the stream.

        The StreamServer will automatically call this method when it
        is stopped. User code won't probably need to call it.

        """
        self.dispatch_event(events.create_command(self.source_id,
                                                  'Stream-Finished'))
        if self.buffering_time:
            self.buffer_dump_sched.stop()
            self._dump_buffer()
        self.dispatcher.close()
        ## self.stats_sched.stop()

    def dispatch_event(self, event):
        """Publishes an event in this stream.

        The event may not be sent immediately to normal clients if the
        server is configured to do buffering. However, it will be sent
        immediately to priority clients.

        """
#        logger.logger.event_published(event)
        self.dispatch_events([event])

    def dispatch_events(self, evs):
        """Publishes a list of events in this stream.

        The events may not be sent immediately to normal clients if
        the stream is configured to do buffering. However, they will
        be sent immediately to priority clients.

        """
#        for e in events:
#            logger.logger.event_published(e)
        if self.event_adapter:
            evs = self.event_adapter(evs)
        self.dispatcher.dispatch_immediate(evs)
        if self.buffering_time is None:
            self.dispatcher.dispatch(evs)
        else:
            self._event_buffer.extend(evs)

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
        self.dispatcher.dispatch(self._event_buffer)
        self._event_buffer = []

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
    def __init__(self, path, streams, allow_publish=False, filter_=None,
                 parse_event_body=False,
                 buffering_time=None,
                 event_adapter=None,
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

        The rest of the parameters are as described in the constructor
        of the Stream class.

        """
        super(RelayStream, self).__init__(path,
                                          allow_publish=allow_publish,
                                          buffering_time=buffering_time,
                                          event_adapter=event_adapter,
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
        for e in evs:
            e.append_aggregator_id(self.source_id)
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

    def send(self, data):
        self.callback(data)

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
    def __init__(self, stream, num_recent_events=2048, ioloop=None):
        self.stream = stream
        self.dispatchers = {}
        self.immediate_dispatchers = []
        self.buffered_dispatchers = []
        self._init_dispatchers()
        self.last_event_time = time.time()
        self._auto_finish = False
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.recent_events = _RecentEventsBuffer(num_recent_events)
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
        logging.info('{}: server cycle; events: {}'.format(self.stream.path,
                                                           len(evs)))
        self.recent_events.append_events(evs)
        if not evs:
            if self._auto_finish and time.time() - self.last_event_time > 60:
                logger.logger.server_closed(self.num_clients)
                self.close()
                self.ioloop.stop()
        else:
            self.last_event_time = time.time()
        pack = dispatchers.EventsPack(evs)
        for dispatcher in self.buffered_dispatchers:
            dispatcher.dispatch(pack)
            if evs and not dispatcher.properties.streaming:
                dispatcher.close()
            for e in evs:
                logger.logger.event_dispatched(e)

    def close(self):
        """Closes every active streaming client."""
        for dispatcher in self.dispatchers.values():
            dispatcher.close()

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


class _GenericHandler(tornado.web.RequestHandler):
    _q_re = re.compile(r'^\s*(\w+)\s*;\s*q=(0(\.[0-9]{1,3})?|1(\.0{1,3})?)$')
    _attribute_re = re.compile(r'^\s*(\w+)\s*')

    def __init__(self, application, request, **kwargs):
        super(_GenericHandler, self).__init__(application, request, **kwargs)

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
        return _GenericHandler._accept_values_internal( \
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
            attribute, quality = _GenericHandler._read_q_value(value)
            q_values.append((-quality, i, attribute))
        return [v[2] for v in sorted(q_values)]

    @staticmethod
    def _read_q_value(value):
        correct = False
        if ';' in value:
            r = _GenericHandler._q_re.search(value)
            if r:
                attribute = r.group(1)
                try:
                    quality = float(r.group(2))
                    correct = True
                except ValueError:
                    # correct remains False
                    pass
        else:
            r = _GenericHandler._attribute_re.search(value)
            if r:
                attribute = r.group(1)
                quality = 1.0
                correct = True
        if  correct:
            return attribute, quality
        else:
            raise tornado.web.HTTPError(400, 'Bad Request')


class _MainHandler(_GenericHandler):
    def get(self):
        raise tornado.web.HTTPError(404)


class _EventPublishHandler(_GenericHandler):
    def __init__(self, application, request, stream=None,
                 stop_when_source_finishes=False):
        super(_EventPublishHandler, self).__init__(application, request)
        self.stream = stream
        self.stop_when_source_finishes = stop_when_source_finishes

    def get(self):
        event_id = self.get_argument('event-id', default=None)
        if event_id is None:
            event_id = ztreamy.random_id()
        source_id = self.get_argument('source-id')
        syntax = self.get_argument('syntax')
        application_id = self.get_argument('application_id')
        body = self.get_argument('body')
        aggregator_id = events.parse_aggregator_id( \
            self.get_argument('aggregator-id', default=''))
        event_type = self.get_argument('event-type', default=None)
        timestamp = self.get_argument('timestamp', default=None)
        event = events.Event(source_id, syntax, body,
                             application_id=application_id,
                             aggregator_id=aggregator_id,
                             event_type=event_type, timestamp=timestamp)
        event.aggregator_id.append(self.stream.source_id)
        self.stream.dispatch_event(event)
        self.finish()

    def post(self):
        if self.request.headers['Content-Type'] == ztreamy.event_media_type:
            deserializer = events.Deserializer()
            parse_body = self.stream.parse_event_body
        elif self.request.headers['Content-Type'] == ztreamy.json_media_type:
            deserializer = events.JSONDeserializer()
            parse_body = True
        else:
            raise tornado.web.HTTPError(400, 'Bad content type')
        try:
            evs = deserializer.deserialize(self.request.body,
                                           parse_body=parse_body,
                                           complete=True)
        except Exception as ex:
            traceback.print_exc()
            raise tornado.web.HTTPError(400, str(ex))
        for event in evs:
            if event.syntax == 'ztreamy-command':
                if event.command == 'Event-Source-Finished':
                    if self.stop_when_source_finishes:
                        self.stream._finish_when_possible()
            event.aggregator_id.append(self.stream.source_id)
            self.stream.dispatch_event(event)
        self.finish()


class _EventStreamHandler(_GenericHandler):
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
            if serialization == ztreamy.SERIALIZATION_ZTREAMY:
                self.set_header('Content-Type', ztreamy.stream_media_type)
            else:
                self.set_header('Content-Type', ztreamy.ldjson_media_type)
            # Allow cross-origin with CORS (see http://www.w3.org/TR/cors/):
            self.set_header('Access-Control-Allow-Origin', '*')
            if encoding == ClientProperties.ENCODING_ZLIB:
                self.set_header('Content-Encoding', 'deflate')
        else:
            raise tornado.web.HTTPError(406, 'Not Acceptable')

    def _on_new_data(self, data):
        if self.request.connection.stream.closed():
            self.dispatcher.deregister_client(self.client)
            return
        self.write(data)
        self.flush()


class _ShortLivedHandler(_GenericHandler):
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

    def _on_new_data(self, data):
        if not self.request.connection.stream.closed():
            self.write(data)


class _RecentEventsBuffer(object):
    """A circular buffer that stores the latest events of a stream."""
    def __init__(self, size):
        """Creates a new buffer with capacity for 'size' events."""
        self.buffer = [None] * size
        self.position = 0
        self.events = {}

    def append_event(self, event):
        """Appends an event to the buffer."""
        if self.buffer[self.position] is not None:
            del self.events[self.buffer[self.position].event_id]
        self.buffer[self.position] = event
        self.events[event.event_id] = self.position
        self.position += 1
        if self.position == len(self.buffer):
            self.position = 0

    def append_events(self, events):
        """Appends a list of events to the buffer."""
        if len(events) > len(self.buffer):
            events = events[-len(self.buffer):]
        if self.position + len(events) >= len(self.buffer):
            first_block = len(self.buffer) - self.position
            self._append_internal(events[:first_block])
            self._append_internal(events[first_block:])
        else:
            self._append_internal(events)

    def load_from_file(self, file_):
        deserializer = events.Deserializer()
        for evs in deserializer.deserialize_file(file_):
            self.append_events(evs)

    def newer_than(self, event_id, limit=None):
        """Returns the events newer than the given 'event_id'.

        If no event in the buffer has the 'event_id', all the events are
        returned. If the newest event in the buffer has that id, an empty
        list is returned.

        If 'limit' is not None, at most 'limit' events are returned
        (the most recent ones).

        Returns a tuple ('events', 'complete') where 'events' is the list
        of events and 'complete' is True when 'event_id' is in the buffer
        and no limit was applied.

        """
        if event_id in self.events:
            complete = True
            pos = self.events[event_id] + 1
            if pos == len(self.buffer):
                pos = 0
            if pos == self.position:
                data = []
            elif pos < self.position:
                data = self.buffer[pos:self.position]
            else:
                data = self.buffer[pos:] + self.buffer[:self.position]
        else:
            complete = False
            if (self.position != len(self.buffer) - 1
                and self.buffer[self.position + 1] is None):
                data = self.buffer[:self.position]
            else:
                data = (self.buffer[self.position:]
                        + self.buffer[:self.position])
        if limit is not None and len(data) > limit:
            data = data[-limit:]
            complete = False
        return data, complete

    def most_recent(self, num_events):
        if num_events > len(self.buffer):
            num_events = len(self.buffer)
        pos = self.position - num_events
        if pos >= 0:
            data = self.buffer[pos:self.position]
        elif self.buffer[-1] is not None:
            data = self.buffer[pos:] + self.buffer[:self.position]
        else:
            data = self.buffer[:self.position]
        return data

    def _append_internal(self, events):
        self._remove_from_dict(self.position, len(events))
        self.buffer[self.position:self.position + len(events)] = events
        for i in range(0, len(events)):
            self.events[events[i].event_id] = self.position + i
        self.position = (self.position + len(events)) % len(self.buffer)

    def _remove_from_dict(self, position, num_events):
        for i in range(position, position + num_events):
            if self.buffer[i] is not None:
                if self.buffer[i].event_id in self.events:
                    del self.events[self.buffer[i].event_id]

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
    tornado.options.define('eventlog', default=False,
                           help='dump event log',
                           type=bool)
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
    if tornado.options.options.eventlog:
        print(stream.source_id)
        comments = {'Buffer time (ms)': buffering_time}
#        logger.logger = logger.ZtreamyLogger(stream.source_id,
        logger.logger = logger.CompactServerLogger(stream.source_id,
                                                   'server-' + stream.source_id
                                                   + '.log', comments)
        logger.logger.auto_flush = True
     # Uncomment to test Stream.stop():
#    tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 5, stop_server)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
        logger.logger.close()


if __name__ == "__main__":
    main()
