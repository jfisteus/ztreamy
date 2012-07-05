""" Code for event stream servers.

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
    def __init__(self, port, ioloop=None, allow_publish=False,
                 buffering_time=None, source_id=None):
        logging.info('Initializing server...')
        if source_id is not None:
            self.source_id = source_id
        else:
            self.source_id = streamsem.random_id()
        self.port = port
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.app = WebApplication(self, allow_publish=allow_publish)
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
#        logger.logger.event_published(event)
        self.app.dispatcher.dispatch_priority([event])
        if self.buffering_time is None:
            self.app.dispatcher.dispatch([event])
        else:
            self._event_buffer.append(event)

    def dispatch_events(self, events):
#        for e in events:
#            logger.logger.event_published(e)
        self.app.dispatcher.dispatch_priority(events)
        if self.buffering_time is None:
            self.app.dispatcher.dispatch(events)
        else:
            self._event_buffer.extend(events)

    def start(self, loop=True):
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
    """A server that relays events from other servers."""
    def __init__(self, port, source_urls, ioloop=None,
                 allow_publish=False, filter_=None,
                 buffering_time=None):
        super(RelayServer, self).__init__(port, ioloop=ioloop,
                                          allow_publish=allow_publish,
                                          buffering_time=buffering_time)
        if filter_ is not None:
            filter_.callback = self._relay_events
            event_callback = filter_.filter_events
        else:
            event_callback = self._relay_events
        self.source_urls = source_urls
        self.clients = \
            [AsyncStreamingClient(url, event_callback=event_callback,
                                  error_callback=self._handle_error,
                                  parse_event_body=False,
                                  separate_events=False) \
                 for url in source_urls]

    def start(self, loop=True):
        for client in self.clients:
            client.start(loop=False)
        super(RelayServer, self).start(loop)

    def stop(self):
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


class WebApplication(tornado.web.Application):
    def __init__(self, server, allow_publish=False):
        self.dispatcher = EventDispatcher()
        handler_kwargs = {
            'server': server,
            'dispatcher': self.dispatcher,
        }
        compressed_kwargs = {'compress': True}
        compressed_kwargs.update(handler_kwargs)
        priority_kwargs = {'compress': False, 'priority': True}
        priority_kwargs.update(handler_kwargs)
        handlers = [
            tornado.web.URLSpec(r"/", MainHandler),
            tornado.web.URLSpec(r"/events/stream", EventStreamHandler,
                                kwargs=handler_kwargs),
            tornado.web.URLSpec(r"/events/compressed", EventStreamHandler,
                                kwargs=compressed_kwargs),
            tornado.web.URLSpec(r"/events/priority", EventStreamHandler,
                                kwargs=priority_kwargs),
            tornado.web.URLSpec(r"/events/next", NextEventHandler,
                                kwargs=handler_kwargs),
        ]
        if allow_publish:
            publish_kwargs = {'server': server}
            handlers.append(tornado.web.URLSpec(r"/events/publish",
                                                EventPublishHandler,
                                                kwargs=publish_kwargs))
        # No settings by now...
        settings = dict()
        super(WebApplication, self).__init__(handlers, **settings)


class Client(object):
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


class EventDispatcher(object):
    def __init__(self):
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

    def register_client(self, client):
        if client.streaming:
            if client.priority:
                self.priority_clients.append(client)
            elif client.compress:
                self.unsynced_compressed_streaming_clients.append(client)
            else:
                self.streaming_clients.append(client)
            logging.info('Streaming client registered; stream: %i; comp: %i'\
                             %(client.streaming, client.compress))
        else:
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


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        raise tornado.web.HTTPError(404)


class EventPublishHandler(tornado.web.RequestHandler):
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


class EventStreamHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, server=None,
                 dispatcher=None, compress=False, priority=False):
        tornado.web.RequestHandler.__init__(self, application, request)
        self.dispatcher = dispatcher
        self.compress = compress
        self.server = server
        self.priority = priority

    @tornado.web.asynchronous
    def get(self):
        self.client = Client(self, self._on_new_data, streaming=True,
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


class NextEventHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, dispatcher=None):
        tornado.web.RequestHandler.__init__(self, application, request)
        self.dispatcher = dispatcher

    @tornado.web.asynchronous
    def get(self):
        self.client = Client(self.on_new_event, streaming=False)
        self.dispatcher.register_client(self.client)

    def on_new_event(self, event):
        if not self.request.connection.stream.closed():
            self.write(str(event))
            self.finish()

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
