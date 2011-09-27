""" Code for event stream servers.

"""

import logging
import tornado.escape
import tornado.ioloop
import tornado.web
import tornado.httpserver

import streamsem
from streamsem import events
from streamsem import StreamsemException
from streamsem.client import AsyncStreamingClient

import streamsem

class StreamServer(object):
    def __init__(self, port, ioloop=None, allow_publish=False,
                 buffering_time=None):
        logging.info('Initializing server...')
        self.port = port
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.app = WebApplication(allow_publish=allow_publish)
        self.http_server = tornado.httpserver.HTTPServer(self.app)
        self.buffering_time = buffering_time
        self._event_buffer = []
        if buffering_time:
            self.buffer_dump_sched = \
                tornado.ioloop.PeriodicCallback(self._dump_buffer,
                                                buffering_time,
                                                self.ioloop)
        self._looping = False
        self._started = False
        self._stopped = False

    def dispatch_event(self, event):
        if self.buffering_time is None:
            self.app.dispatcher.dispatch([event])
        else:
            self._event_buffer.append(event)

    def dispatch_events(self, events):
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
        if loop:
            self._looping = True
            self._started = True
            self.ioloop.start()
            self._looping = False

    def stop(self):
        if self._started and not self._stopped:
            logging.info('Stopping server...')
            self.http_server.stop()
            self.app.dispatcher.close()
            if self.buffering_time:
                self.buffer_dump_sched.stop()
            self._stopped = True
            if self._looping == True:
                self.ioloop.stop()
                self._looping = False

    def _dump_buffer(self):
        self.app.dispatcher.dispatch(self._event_buffer)
        self._event_buffer = []

class RelayServer(StreamServer):
    """A server that relays events from other servers."""
    def __init__(self, port, source_urls, aggregator_id, ioloop=None,
                 allow_publish=False):
        super(RelayServer, self).__init__(port, ioloop=ioloop,
                                          allow_publish=allow_publish)
        if aggregator_id is not None:
            self.aggregator_id = aggregator_id
        else:
            self.aggregator_id = streamsem.random_id()
        self.source_urls = source_urls
        self.clients = \
            [AsyncStreamingClient(url, event_callback=self._relay_event,
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

    def _relay_event(self, events):
        for e in events:
            e.append_aggregator_id(self.aggregator_id)
        self.dispatch_events(events)

    def _handle_error(self, message, http_error=None):
        if http_error is not None:
            logging.error(message + ': ' + str(http_error))
        else:
            logging.error(message)


class WebApplication(tornado.web.Application):
    def __init__(self, allow_publish=False):
        self.dispatcher = EventDispatcher()
        handler_kwargs = {
            'dispatcher': self.dispatcher,
        }
        handlers = [
            tornado.web.URLSpec(r"/", MainHandler),
            tornado.web.URLSpec(r"/events/stream", EventStreamHandler,
                                kwargs=handler_kwargs),
            tornado.web.URLSpec(r"/events/next", NextEventHandler,
                                kwargs=handler_kwargs),
        ]
        if allow_publish:
            handlers.append(tornado.web.URLSpec(r"/events/publish",
                                                EventPublishHandler,
                                                kwargs=handler_kwargs))
        # No settings by now...
        settings = dict()
        super(WebApplication, self).__init__(handlers, **settings)


class Client(object):
    def __init__(self, handler, callback, streaming):
        self.handler = handler
        self.callback = callback
        self.streaming = streaming

    def close(self):
        """Closes the connection to this client.

        It does nothing if the connection is already closed.

        """
        if not self.handler.request.connection.stream.closed():
            logging.info('Finishing a client...')
            self.handler.finish()


class EventDispatcher(object):
    def __init__(self):
        self.streaming_clients = []
        self.one_time_clients = []
        self.event_cache = []
        self.cache_size = 200

    def register_client(self, client):
        if client.streaming:
            self.streaming_clients.append(client)
            logging.info('Streaming client registered')
        else:
            self.one_time_clients.append(client)

    def deregister_client(self, client):
        if client.streaming and client in self.streaming_clients:
            self.streaming_clients.remove(client)
            logging.info('Client deregistered')
        else:
            logging.info('Client already deregistered')

    def dispatch(self, events):
        num_clients = len(self.streaming_clients) + len(self.one_time_clients)
        logging.info('Sending %r events to %r clients', len(events),
                     num_clients)
        if num_clients > 0:
            if isinstance(events, list):
                data = []
                for e in events:
                    if not isinstance(e, streamsem.events.Event):
                        raise StreamsemException('Bad event type',
                                                 'send_event')
                    data.append(str(e))
                serialized = ''.join(data)
            else:
                raise StreamsemException('Bad event type', 'send_event')
            for client in self.streaming_clients:
                try:
                    client.callback(serialized)
                except:
                    logging.error("Error in client callback", exc_info=True)
            for client in self.one_time_clients:
                try:
                    client.callback(serialized)
                except:
                    logging.error("Error in client callback", exc_info=True)
        self.one_time_clients = []
        self.event_cache.extend(events)
        if len(self.event_cache) > self.cache_size:
            self.event_cache = self.event_cache[-self.cache_size:]

    def close(self):
        """Closes every active streaming client."""
        for client in self.streaming_clients:
            client.close()
        self.streaming_clients = []


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        raise tornado.web.HTTPError(404)


class EventPublishHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, dispatcher=None):
        tornado.web.RequestHandler.__init__(self, application, request)
        self.dispatcher = dispatcher

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
        self.dispatcher.dispatch(event)
        self.finish()

    def post(self):
        if self.request.headers['Content-Type'] != streamsem.mimetype_event:
            raise tornado.web.HTTPError(400, 'Bad content type')
        try:
            events = events.deserialize(self.request.body)
        except Exception as ex:
            raise tornado.web.HTTPError(400, str(ex))
        for event in events:
            self.dispatcher.dispatch(event)
        self.finish()


class EventStreamHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, dispatcher=None):
        tornado.web.RequestHandler.__init__(self, application, request)
        self.dispatcher = dispatcher

    @tornado.web.asynchronous
    def get(self):
        self.client = Client(self, self._on_new_data, True)
        self.dispatcher.register_client(self.client)

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
        self.client = Client(self.on_new_event, False)
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

    def publish_event():
        logging.info('In publish_event')
        event_body = ('<http://example.com/now> '
                       '<http://example.com/time> "%s".'%time.time())
        event = rdfevents.RDFEvent(source_id, 'n3', event_body,
                                   application_id=application_id)
        server.dispatch_event(event)
    def stop_server():
        server.stop()

    tornado.options.define('port', default=8888, help='run on the given port',
                           type=int)
    tornado.options.parse_command_line()
    port = tornado.options.options.port
    server = StreamServer(port, allow_publish=True, buffering_time=5000)
    sched = tornado.ioloop.PeriodicCallback(publish_event, 3000,
                                            io_loop=server.ioloop)
    sched.start()

     # Uncomment to test StreamServer.stop():
#    tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 5, stop_server)

    server.start()


if __name__ == "__main__":
    main()
