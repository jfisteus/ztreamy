# streamsem: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2012 Jesus Arias Fisteus
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
""" Implementation of stream servers.

Two servers are provided: 'StreamServer' and 'RelayServer'.

'StreamServer' is a basic server for a stream of events. Right now,
each server can serve only one stream, although this limitation will
be removed in the future. It can transmit events from remote sources
or generated at the process of the server.

'RelayServer' extends the basic server with functionality to listen to
other streams and retransmitting their events. Events from remote
sources or generated at the process of the server can also be
published with this server in the stream.

"""

import logging
import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httpserver
import zlib
import traceback
import time

import streamsem
from streamsem import events
from streamsem import StreamsemException
from streamsem.client import AsyncStreamingClient
from streamsem import logger

param_max_events_sync = 20

# Uncomment to do memory profiling
#import guppy.heapy.RM

class StreamServer(object):
    """Serves a stream of events through HTTP.

    Clients connect to this server in order to listen to the events
    it sends. The server provides several HTTP paths which clients
    may use:

    '/': not implemented yet. Intended for human readable
        information about the stream.

    '/events/stream': uncompressed stream with long-lived response.
        The response is not closed by the server, and events are sent
        to the client as they are available. A client library able
        to notify the user as new chunks of data are received is needed
        in order to follow this stream.

    '/events/compressed': zlib-compressed stream with long-lived
        response.  Similar to how the previous path works with the
        only difference of compression.

    '/events/priority': uncompressed stream with lower delays intended
        for clients with priority, typically relay servers.

    '/events/next': available events are sent to the client uncompressed.
        The request is closed immediately. The client can specify the latest
        event it has received in order to get the events that follow it.

    '/events/publish': receives events from event sources in order to be
        published in the stream.

    """
    def __init__(self, port, ioloop=None, allow_publish=False,
                 buffering_time=None, source_id=None,
                 num_recent_events=2048):
        """Creates a stream server object.

        The server object will to start to process requests until
        the 'start()' method is invoked.

        The server listens on the given 'port' for HTTP requests. It
        does not accept events from clients, unless 'allow_publish' is
        set to 'True'.

        'buffering_time' controls the period for which events are
        accumulated in a buffer before sending them to the
        clients. Higher times improve CPU performance in the server
        and compression ratios, but increase the latency in the
        delivery of events.

        If a 'ioloop' object is given, the client will block on it
        apon calling the 'start()' method. If not, it will block on
        the default 'ioloop' of Tornado.

        """
        logging.info('Initializing server...')
        if source_id is not None:
            self.source_id = source_id
        else:
            self.source_id = streamsem.random_id()
        self.port = port
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.app = _WebApplication(self, allow_publish=allow_publish)
        self.http_server = tornado.httpserver.HTTPServer(self.app)
        self.buffering_time = buffering_time
        self._event_buffer = []
        if buffering_time:
            self.buffer_dump_sched = \
                tornado.ioloop.PeriodicCallback(self._dump_buffer,
                                                buffering_time,
                                                self.ioloop)
        self.stats_sched = tornado.ioloop.PeriodicCallback( \
            self.app.dispatcher.stats, 10000, self.ioloop)
        self._looping = False
        self._started = False
        self._stopped = False

    def dispatch_event(self, event):
        """Publishes an event in this stream.

        The event may not be sent immediately to normal clients if the
        server is configured to do buffering. However, it will be sent
        immediately to priority clients.

        """
#        logger.logger.event_published(event)
        self.app.dispatcher.dispatch_priority([event])
        if self.buffering_time is None:
            self.app.dispatcher.dispatch([event])
        else:
            self._event_buffer.append(event)

    def dispatch_events(self, events):
        """Publishes a list of events in this stream.

        The events may not be sent immediately to normal clients if
        the server is configured to do buffering. However, they will
        be sent immediately to priority clients.

        """
#        for e in events:
#            logger.logger.event_published(e)
        self.app.dispatcher.dispatch_priority(events)
        if self.buffering_time is None:
            self.app.dispatcher.dispatch(events)
        else:
            self._event_buffer.extend(events)

    def start(self, loop=True):
        """Starts the server.

        The server begins to listen to HTTP requests.

        If 'loop' is true (which is the default), the server will
        block on the ioloop until 'close()' is called.

        """
        assert(not self._started)
        logging.info('Starting server...')
        self.http_server.listen(self.port)
        if self.buffering_time:
            self.buffer_dump_sched.start()
        self.stats_sched.start()
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
            self.http_server.stop()
            self.app.dispatcher.close()
            if self.buffering_time:
                self.buffer_dump_sched.stop()
            self.stats_sched.stop()
            self._stopped = True
            if self._looping == True:
                self.ioloop.stop()
                self._looping = False

    def _start_timing(self):
        self._cpu_timer_start = time.clock()
        self._real_timer_start = time.time()

    def _stop_timing(self):
        cpu_timer_stop = time.clock()
        real_timer_stop = time.time()
        cpu_time = cpu_timer_stop - self._cpu_timer_start
        real_time = real_timer_stop - self._real_timer_start
        logger.logger.server_timing(cpu_time, real_time,
                                    self._real_timer_start)

    def _dump_buffer(self):
        self.app.dispatcher.dispatch(self._event_buffer)
        self._event_buffer = []


class RelayServer(StreamServer):
    """A server that relays events from other servers.

    This server listens to other event streams and publishes their
    events as a new stream. A filter can optionally be apply in order
    to select which events are published.

    """
    def __init__(self, port, source_urls, ioloop=None,
                 allow_publish=False, filter_=None,
                 buffering_time=None):
        """Creates a new server, but does not start it.

        The server will transmit the events of the streams specified
        in 'source_urls' (either a list or a string with just one
        URL).  A filter may be specified in 'filter_' in order to
        select which events are relayed (see the 'filters' module for
        more information).

        The rest of the parameters are as described in the constructor
        of the StreamServer class.

        """
        super(RelayServer, self).__init__(port, ioloop=ioloop,
                                          allow_publish=allow_publish,
                                          buffering_time=buffering_time)
        if filter_ is not None:
            filter_.callback = self._relay_events
            event_callback = filter_.filter_events
        else:
            event_callback = self._relay_events
        if isinstance(source_urls, basestring):
            self.source_urls = [source_urls]
        else:
            self.source_urls = source_urls
        self.clients = \
            [AsyncStreamingClient(url, event_callback=event_callback,
                                  error_callback=self._handle_error,
                                  parse_event_body=False,
                                  separate_events=False) \
                 for url in source_urls]

    def start(self, loop=True):
        """Starts the relay server.

        See 'start()' in StreamServer for more information.

        """
        for client in self.clients:
            client.start(loop=False)
        super(RelayServer, self).start(loop)

    def stop(self):
        """Stops the relay server.

        See 'start()' in StreamServer for more information.

        """
        if self._started and not self._stopped:
            for client in self.clients:
                client.stop()
            super(RelayServer, self).stop()

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


class _WebApplication(tornado.web.Application):
    def __init__(self, server, allow_publish=False):
        self.dispatcher = _EventDispatcher()
        handler_kwargs = {
            'server': server,
            'dispatcher': self.dispatcher,
        }
        compressed_kwargs = {'compress': True}
        compressed_kwargs.update(handler_kwargs)
        priority_kwargs = {'compress': False, 'priority': True}
        priority_kwargs.update(handler_kwargs)
        handlers = [
            tornado.web.URLSpec(r"/", _MainHandler),
            tornado.web.URLSpec(r"/events/stream", _EventStreamHandler,
                                kwargs=handler_kwargs),
            tornado.web.URLSpec(r"/events/compressed", _EventStreamHandler,
                                kwargs=compressed_kwargs),
            tornado.web.URLSpec(r"/events/priority", _EventStreamHandler,
                                kwargs=priority_kwargs),
            tornado.web.URLSpec(r"/events/short-lived", _ShortLivedHandler,
                                kwargs=handler_kwargs),
        ]
        if allow_publish:
            publish_kwargs = {'server': server}
            handlers.append(tornado.web.URLSpec(r"/events/publish",
                                                _EventPublishHandler,
                                                kwargs=publish_kwargs))
        # No settings by now...
        settings = dict()
        super(_WebApplication, self).__init__(handlers, **settings)


class _Client(object):
    def __init__(self, handler, callback, streaming=False, compress=False,
                 priority=False):
        assert streaming or not compress
        self.handler = handler
        self.callback = callback
        self.streaming = streaming
        if not priority:
            self.compress = compress
        else:
            self.compress = False
        self.priority = priority
        self.closed = False
        if self.compress:
            self.compression_synced = False
            self.compressor = zlib.compressobj()

    def send(self, data):
        if self.compress and not self.compression_synced:
            comp_data = self.compressor.compress(data)
            comp_data_extra = self.compressor.flush(zlib.Z_SYNC_FLUSH)
            self.callback(comp_data + comp_data_extra)
        else:
            self.callback(data)

    def close(self):
        """Closes the connection to this client.

        It does nothing if the connection is already closed.

        """
        self.closed = True
        if not self.handler.request.connection.stream.closed():
            logging.info('Finishing a client...')
            self.handler.finish()
            self.compressor = None

    def sync_compression(self):
        """Places the object in synced state."""
        if self.compress and not self.compression_synced:
            self.send('')
            self.compressor = None
            self.compression_synced = True


class _EventDispatcher(object):
    def __init__(self, num_recent_events=2048):
        self.priority_clients = []
        self.streaming_clients = []
        self.compressed_streaming_clients = []
        self.unsynced_compressed_streaming_clients = []
        self.one_time_clients = []
        self.event_cache = []
        self.cache_size = 200
        self._compressor = zlib.compressobj()
        self._num_events_since_sync = 0
        self._next_client_cleanup = -1
        self._periods_since_last_event = 0
        self.sent_bytes = 0
        self.recent_events = _RecentEventsBuffer(num_recent_events)

    def register_client(self, client, last_event_seen=None):
        if client.streaming:
            if client.priority:
                self.priority_clients.append(client)
            elif client.compress:
                self.unsynced_compressed_streaming_clients.append(client)
            else:
                self.streaming_clients.append(client)
            logging.info('Streaming client registered; stream: %i; comp: %i'\
                             %(client.streaming, client.compress))
        if last_event_seen != None:
            # Send the available events after the last seen event
            evs, none_lost = self.recent_events.newer_than(last_event_seen)
            if len(evs) > 0:
                client.send(self._serialize_events(evs))
                if not client.streaming:
                    client.close()
        if not client.streaming and not client.closed:
            self.one_time_clients.append(client)

    def deregister_client(self, client):
        if client.streaming:
            client.closed = True
            logging.info('Client deregistered')
            if self._next_client_cleanup < 0:
                self._next_client_cleanup = 10

    def clean_closed_clients(self):
        self.priority_clients = \
            [c for c in self.priority_clients if not c.closed]
        self.streaming_clients = \
            [c for c in self.streaming_clients if not c.closed]
        self.compressed_streaming_clients = \
            [c for c in self.compressed_streaming_clients if not c.closed]
        self.unsynced_compressed_streaming_clients = \
            [c for c in self.unsynced_compressed_streaming_clients \
                 if not c.closed]
        self._next_client_cleanup = -1
        logging.info('Cleaning up clients')

    def dispatch_priority(self, evs):
        if len(self.priority_clients) > 0 and len(evs) > 0:
            serialized = self._serialize_events(evs)
            for client in self.priority_clients:
                self._send(serialized, client)

    def dispatch(self, evs):
        num_clients = (len(self.streaming_clients) + len(self.one_time_clients)
                       + len(self.unsynced_compressed_streaming_clients)
                       + len(self.compressed_streaming_clients))
        logging.info('Sending %r events to %r clients', len(evs),
                     num_clients)
        self.recent_events.append_events(evs)
        self._next_client_cleanup -= 1
        if self._next_client_cleanup == 0:
            self.clean_closed_clients()
        if isinstance(evs, list):
            if evs == [] and (num_clients > 0
                              or len(self.priority_clients) > 0):
                self._periods_since_last_event += 1
                if self._periods_since_last_event > 20:
                    logging.info('Sending Test-Connection event')
                    evs = [events.Command('', 'streamsem-command',
                                          'Test-Connection')]
                    self._periods_since_last_event = 0
                    self.dispatch_priority(evs)
                else:
                    return
        else:
            raise StreamsemException('Bad event type', 'send_event')
        self._periods_since_last_event = 0
        if len(self.unsynced_compressed_streaming_clients) > 0:
            if (len(self.compressed_streaming_clients) == 0
                or self._num_events_since_sync > param_max_events_sync):
                self._sync_compressor()
        if num_clients > 0:
            logging.info('Compressed clients: %d synced; %d unsynced'%\
                             (len(self.compressed_streaming_clients),
                              len(self.unsynced_compressed_streaming_clients)))
            serialized = self._serialize_events(evs)
            for client in self.streaming_clients:
                self._send(serialized, client)
            for client in self.unsynced_compressed_streaming_clients:
                self._send(serialized, client)
            for client in self.one_time_clients:
                self._send(serialized, client)
                client.close()
            if len(self.compressed_streaming_clients) > 0:
                compressed_data = (self._compressor.compress(serialized)
                                   + self._compressor.flush(zlib.Z_SYNC_FLUSH))
                for client in self.compressed_streaming_clients:
                    self._send(compressed_data, client)
            for e in evs:
                logger.logger.event_dispatched(e)
        self.one_time_clients = []
        self.event_cache.extend(evs)
        if len(self.event_cache) > self.cache_size:
            self.event_cache = self.event_cache[-self.cache_size:]
        self._num_events_since_sync += len(evs)

    def close(self):
        """Closes every active streaming client."""
        for client in self.streaming_clients:
            client.close()
        self.streaming_clients = []
        self.stats()

    def stats(self):
        logger.logger.server_traffic_sent(time.time(), self.sent_bytes)
        self.sent_bytes = 0

    def _serialize_events(self, evs):
        data = []
        for e in evs:
            if not isinstance(e, events.Event):
                raise StreamsemException('Bad event type', 'send_event')
            data.append(str(e))
        return ''.join(data)

    def _sync_compressor(self):
        for client in self.unsynced_compressed_streaming_clients:
            client.sync_compression()
        data = self._compressor.flush(zlib.Z_FULL_FLUSH)
        if len(data) > 0:
            for client in self.compressed_streaming_clients:
                self._send(data, client)
        self.compressed_streaming_clients.extend( \
            self.unsynced_compressed_streaming_clients)
        self.unsynced_compressed_streaming_clients = []
        self._num_events_since_sync = 0
        logging.info('Compressor synced')

    def _send(self, data, client):
        try:
            if not client.closed:
                client.send(data)
                self.sent_bytes += len(data)
        except:
            logging.error("Error in client callback", exc_info=True)


class _MainHandler(tornado.web.RequestHandler):
    def get(self):
        raise tornado.web.HTTPError(404)


class _EventPublishHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, server=None):
        tornado.web.RequestHandler.__init__(self, application, request)
        self.server = server

    def get(self):
        event_id = self.get_argument('event-id', default=None)
        if event_id is None:
            event_id = streamsem.random_id()
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
        event.aggregator_id.append(self.server.source_id)
        self.server.dispatch_event(event)
        self.finish()

    def post(self):
        if self.request.headers['Content-Type'] != streamsem.mimetype_event:
            raise tornado.web.HTTPError(400, 'Bad content type')
        deserializer = events.Deserializer()
        try:
            evs = deserializer.deserialize(self.request.body, parse_body=False,
                                           complete=True)
        except Exception as ex:
            traceback.print_exc()
            raise tornado.web.HTTPError(400, str(ex))
        for event in evs:
            if event.syntax == 'streamsem-command':
                if event.command == 'Event-Source-Started':
                    self.server._start_timing()
                elif event.command == 'Event-Source-Finished':
                    self.server._stop_timing()
            event.aggregator_id.append(self.server.source_id)
            self.server.dispatch_event(event)
        self.finish()


class _EventStreamHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, server=None,
                 dispatcher=None, compress=False, priority=False):
        tornado.web.RequestHandler.__init__(self, application, request)
        self.dispatcher = dispatcher
        self.compress = compress
        self.server = server
        self.priority = priority

    @tornado.web.asynchronous
    def get(self):
        self.client = _Client(self, self._on_new_data, streaming=True,
                              compress=self.compress, priority=self.priority)
        self.dispatcher.register_client(self.client)
        if self.compress:
            command = events.create_command(self.server.source_id,
                                            'Set-Compression')
            self._on_new_data(str(command))

    def _on_new_data(self, data):
        if self.request.connection.stream.closed():
            self.dispatcher.deregister_client(self.client)
            return
        self.write(data)
        self.flush()

    def _on_client_close(self):
        self.dispatcher.deregister_client(self.client)


class _ShortLivedHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, dispatcher=None, server=None):
        tornado.web.RequestHandler.__init__(self, application, request)
        self.dispatcher = dispatcher
        # 'server' argument is ignored (not necessary)

    @tornado.web.asynchronous
    def get(self):
        last_event_seen = self.get_argument('last-seen', default=None)
        self.client = _Client(self, self._on_new_data, streaming=False)
        self.dispatcher.register_client(self.client,
                                        last_event_seen=last_event_seen)

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
        if self.position + len(events) >= len(self.buffer):
            first_block = len(self.buffer) - self.position
            self._append_internal(events[:first_block])
            self._append_internal(events[first_block:])
        else:
            self._append_internal(events)

    def newer_than(self, event_id):
        """Returns the events newer than the given 'event_id'.

        If no event in the buffer has the 'event_id', all the events are
        returned. If the newest event in the buffer has that id, an empty
        list is returned.

        Returns a tuple ('events', 'complete') where 'events' is the list
        of events and 'complete' is True when 'event_id' is in the buffer.

        """
        if event_id in self.events:
            pos = self.events[event_id] + 1
            if pos == len(self.buffer):
                pos = 0
            if pos == self.position:
                return [], True
            elif pos < self.position:
                return self.buffer[pos:self.position], True
            else:
                return self.buffer[pos:] + self.buffer[:self.position], True
        else:
            if (self.position != len(self.buffer) - 1
                and self.buffer[self.position + 1] is None):
                return self.buffer[:self.position], False
            else:
                return (self.buffer[self.position:]
                        + self.buffer[:self.position]), False

    def _append_internal(self, events):
        self._remove_from_dict(self.position, len(events))
        self.buffer[self.position:self.position + len(events)] = events
        for i in range(0, len(events)):
            self.events[events[i].event_id] = self.position + i
        self.position = (self.position + len(events)) % len(self.buffer)

    def _remove_from_dict(self, position, num_events):
        for i in range(position, position + num_events):
            if self.buffer[i] is not None:
                del self.events[self.buffer[i].event_id]

def main():
    import time
    import tornado.options
    from streamsem import rdfevents
    source_id = streamsem.random_id()
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
    tornado.options.parse_command_line()
    port = tornado.options.options.port
    if (tornado.options.options.buffer is not None
        and tornado.options.options.buffer > 0):
        buffering_time = tornado.options.options.buffer * 1000
    else:
        buffering_time = None
    server = StreamServer(port, allow_publish=True,
                          buffering_time=buffering_time)
    if tornado.options.options.eventlog:
        print server.source_id
        comments = {'Buffer time (ms)': buffering_time}
#        logger.logger = logger.StreamsemLogger(server.source_id,
        logger.logger = logger.CompactServerLogger(server.source_id,
                                                   'server-' + server.source_id
                                                   + '.log', comments)
     # Uncomment to test StreamServer.stop():
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
