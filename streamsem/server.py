""" Code for event stream servers.

"""

import logging
import time
import tornado.escape
import tornado.ioloop
import tornado.web

import streamsem
from streamsem import events
from streamsem.client import AsyncStreamingClient

import streamsem

class StreamServer(object):
    def __init__(self, port, ioloop=None):
        logging.info('Initializing server...')
        if ioloop is not None:
            self.ioloop = ioloop
        else:
            self.ioloop = tornado.ioloop.IOLoop.instance()
        self.app = Application()
        self.app.listen(port)

    def dispatch_event(self, event):
        self.app.dispatcher.dispatch(event)

    def start(self):
        logging.info('Starting server...')
        self.ioloop.start()


class RelayServer(StreamServer):
    """A server that relays events from other servers."""
    def __init__(self, port, source_urls, aggregator_id, ioloop=None):
        super(RelayServer, self).__init__(port, ioloop=ioloop)
        if aggregator_id is not None:
            self.aggregator_id = aggregator_id
        else:
            self.aggregator_id = streamsem.random_id()
        self.source_urls = source_urls
        self.clients = \
            [AsyncStreamingClient(url, event_callback=self._relay_event,
                                  error_callback=self._handle_error) \
                 for url in source_urls]

    def start(self):
        for client in self.clients:
            client.start(loop=False)
        super(RelayServer, self).start()

    def _relay_event(self, event):
        event.append_aggregator_id(self.aggregator_id)
        self.dispatch_event(event)

    def _handle_error(self, message, http_error=None):
        if http_error is not None:
            logging.error(message + ': ' + str(http_error))
        else:
            logging.error(message)


class Application(tornado.web.Application):
    def __init__(self):
        self.dispatcher = EventDispatcher()
        handler_kwargs = {
            'dispatcher': self.dispatcher,
        }
        handlers = [
            tornado.web.URLSpec(r"/", MainHandler),
            tornado.web.URLSpec(r"/events/publish", EventPublishHandler,
                                kwargs=handler_kwargs),
            tornado.web.URLSpec(r"/events/stream", EventStreamHandler,
                                kwargs=handler_kwargs),
            tornado.web.URLSpec(r"/events/next", NextEventHandler,
                                kwargs=handler_kwargs),
        ]
        # No settings by now...
        settings = dict()
        super(Application, self).__init__(handlers, **settings)


class Client(object):
    def __init__(self, callback, streaming):
        self.callback = callback
        self.streaming = streaming


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

    def dispatch(self, event):
        logging.info('Sending event to %r clients',
                     len(self.streaming_clients) + len(self.one_time_clients))
        for client in self.streaming_clients:
            try:
                client.callback(event)
            except:
                logging.error("Error in client callback", exc_info=True)
        for client in self.one_time_clients:
            try:
                client.callback(event)
            except:
                logging.error("Error in client callback", exc_info=True)
        self.one_time_clients = []
        self.event_cache.append(event)
        if len(self.event_cache) > self.cache_size:
            self.event_cache = self.event_cache[-self.cache_size:]


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        raise tornado.web.HTTPError(404)


class EventPublishHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, dispatcher=None):
        tornado.web.RequestHandler.__init__(self, application, request)
        self.dispatcher = dispatcher

    def get(self):
        event = events.Event(self.get_argument('message'))
        self.dispatcher.dispatch(event)
        self.finish()


class EventStreamHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, dispatcher=None):
        tornado.web.RequestHandler.__init__(self, application, request)
        self.dispatcher = dispatcher

    @tornado.web.asynchronous
    def get(self):
        self.client = Client(self.on_new_event, True)
        self.dispatcher.register_client(self.client)

    def on_new_event(self, event):
        if self.request.connection.stream.closed():
            self.dispatcher.deregister_client(self.client)
            return
        self.write(str(event))
        self.flush()

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
    source_id = streamsem.random_id()
    def publish_event():
        logging.info('In publish_event')
        event = events.Event(source_id, 'n3',
                             '<http://example.com/now> '
                             '<http://example.com/time> "%s".'%time.time())
        server.dispatch_event(event)

    import tornado.options
    tornado.options.define('port', default=8888, help='run on the given port',
                           type=int)
    tornado.options.parse_command_line()
    port = tornado.options.options.port
    server = StreamServer(port)
    sched = tornado.ioloop.PeriodicCallback(publish_event, 3000,
                                            io_loop=server.ioloop)
    sched.start()
    server.start()


if __name__ == "__main__":
    main()
