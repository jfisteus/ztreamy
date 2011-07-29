#!/usr/bin/env python
#

import logging
import time
import tornado.escape
import tornado.ioloop
import tornado.options
import tornado.web

from tornado.options import define, options

define("port", default=8888, help="run on the given port", type=int)


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/events/publish", EventPublish),
            (r"/events/stream", EventStream),
            (r"/events/next", SendNextEvent),
        ]
        settings = dict(
            cookie_secret="43oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
        )
        tornado.web.Application.__init__(self, handlers, **settings)


class BaseHandler(tornado.web.RequestHandler):
    pass


class MainHandler(BaseHandler):
    def get(self):
        raise tornado.web.HTTPError(404)

class Event(object):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return str(self.message)


class EventDispatcher(object):
    streaming_clients = []
    one_time_clients = []
    event_cache = []
    cache_size = 200

    def register_client(self, client):
        if client.streaming:
            EventDispatcher.streaming_clients.append(client)
            logging.info('Streaming client registered')
        else:
            EventDispatcher.one_time_clients.append(client)

    def deregister_client(self, client):
        if client.streaming and client in EventDispatcher.streaming_clients:
            EventDispatcher.streaming_clients.remove(client)
            logging.info('Client deregistered')

    def dispatch(self, event):
        logging.info('Sending event to %r clients',
                     (len(EventDispatcher.streaming_clients)
                      + len(EventDispatcher.one_time_clients)))
        for client in EventDispatcher.streaming_clients:
            try:
                client.callback(event)
            except:
                logging.error("Error in client callback", exc_info=True)
        for client in EventDispatcher.one_time_clients:
            try:
                client.callback(event)
            except:
                logging.error("Error in client callback", exc_info=True)
        EventDispatcher.one_time_clients = []
        EventDispatcher.event_cache.append(event)
        if len(EventDispatcher.event_cache) > EventDispatcher.cache_size:
            EventDispatcher.event_cache = \
                EventDispatcher.event_cache[-EventDispatcher.cache_size:]


class EventPublish(BaseHandler, EventDispatcher):
    def get(self):
        event = Event(self.get_argument('message'))
        self.dispatch(event)
        self.finish()

class Client(object):
    def __init__(self, callback, streaming):
        self.callback = callback
        self.streaming = streaming


class EventStream(BaseHandler, EventDispatcher):
    @tornado.web.asynchronous
    def get(self):
        self.client = Client(self.on_new_event, True)
        self.register_client(self.client)

    def on_new_event(self, event):
        if self.request.connection.stream.closed():
            self.deregister_client(self.client)
            return
        self.write(str(event))
        self.flush()

class SendNextEvent(BaseHandler, EventDispatcher):
    @tornado.web.asynchronous
    def get(self):
        self.client = Client(self.on_new_event, False)
        self.register_client(self.client)

    def on_new_event(self, event):
        if not self.request.connection.stream.closed():
            self.write(str(event))
            self.finish()

def main():
    tornado.options.parse_command_line()
    app = Application()
    app.listen(options.port)
    io_loop = tornado.ioloop.IOLoop.instance()
    dispatcher = EventDispatcher()

    def publish_event():
        import random
        logging.info('In publish_event')
        event = Event(time.time())
        dispatcher.dispatch(event)

    logging.info('Starting...')
    sched = tornado.ioloop.PeriodicCallback(publish_event, 3000,
                                            io_loop=io_loop)
    sched.start()
    io_loop.start()


if __name__ == "__main__":
    main()
