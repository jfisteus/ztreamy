#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

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
        ]
        settings = dict(
            cookie_secret="43oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
        )
        tornado.web.Application.__init__(self, handlers, **settings)


class BaseHandler(tornado.web.RequestHandler):
    pass


class MainHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        raise tornado.web.HTTPError(404)

class Event(object):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return str(self.message)


class EventDispatcher(object):
    clients = []
    event_cache = []
    cache_size = 200

    def register_client(self, callback):
        EventDispatcher.clients.append(callback)
        logging.info('Client registered')

    def deregister_client(self, callback):
        EventDispatcher.clients.remove(callback)
        logging.info('Client deregistered')

    def dispatch(self, event):
        logging.info('Sending event to %r clients',
                     len(EventDispatcher.clients))
        for callback in EventDispatcher.clients:
            try:
                callback(event)
            except:
                logging.error("Error in client callback", exc_info=True)
        EventDispatcher.event_cache.append(event)
        if len(EventDispatcher.event_cache) > EventDispatcher.cache_size:
            EventDispatcher.event_cache = \
                EventDispatcher.event_cache[-EventDispatcher.cache_size:]


class EventPublish(BaseHandler, EventDispatcher):
    def get(self):
        event = Event(self.get_argument('message'))
        self.dispatch(event)
        self.finish()


class EventStream(BaseHandler, EventDispatcher):
    @tornado.web.asynchronous
    def get(self):
        self.register_client(self.on_new_event)

    def on_new_event(self, event):
        if self.request.connection.stream.closed():
            self.deregister_client(self.on_new_event)
            return
        self.write(str(event))
        self.flush()

def main():
    tornado.options.parse_command_line()
    app = Application()
    app.listen(options.port)
    io_loop = tornado.ioloop.IOLoop.instance()
    dispatcher = EventDispatcher()

    def publish_event():
        logging.info('In publish_event')
        event = Event(time.time())
        dispatcher.dispatch(event)

    logging.info('Starting...')
    sched = tornado.ioloop.PeriodicCallback(publish_event, 5000,
                                            io_loop=io_loop)
    sched.start()
    io_loop.start()


if __name__ == "__main__":
    main()
