import time
import gzip
import tornado.options
import tornado.ioloop

import streamsem
import streamsem.client as client
import streamsem.events as events
import streamsem.logger as logger

class EventPublisher(object):
    def __init__(self, event, publishers):
        self.event = event
        self.publishers = publishers

    def publish(self):
        for publisher in self.publishers:
            publisher.publish(self.event)


class EventScheduler(object):
    def __init__(self, filename, io_loop, publishers,
                 time_scale, compressed=True):
        self.period = 10.0
        self.time_scale = time_scale
        self.publishers = publishers
        self.io_loop = io_loop
        self.finished = False
        self._file_reader = self._read_event_file(filename)
        self.sched = tornado.ioloop.PeriodicCallback(self._schedule_next_events,
                                                     self.period * 1000)
        self.sched.start()
        self._schedule_first_event()

    def _schedule_first_event(self):
        event = self._file_reader.next()
        self.t0_original = event.time()
        self.t0_new = time.time() + 5
        self._schedule_event(event)
        self._schedule_next_events()

    def _schedule_next_events(self):
        try:
            limit = time.time() + 2 * self.period
            while True:
                fire_time = self._schedule_event(self._file_reader.next())
                if fire_time > limit:
                    break
        except StopIteration:
            self.sched.stop()
            self.finished = True

    def _schedule_event(self, event):
        pub = EventPublisher(event, self.publishers)
        fire_time = (self.t0_new
                     + (event.time() - self.t0_original) / self.time_scale)
        self.io_loop.add_timeout(fire_time, pub.publish)
        return fire_time

    def _read_event_file(self, filename):
        if filename.endswith('.gz'):
            file_ = gzip.GzipFile(filename, 'r')
        else:
            file_ = open(filename, 'r')
        deserializer = events.Deserializer()
        while True:
            data = file_.read(1024)
            if data == '':
                break
            evs = deserializer.deserialize(data, parse_body=False,
                                           complete=False)
            for event in evs:
                yield event
        file_.close()


def read_cmd_options():
    from optparse import Values
    tornado.options.define('eventlog', default=False,
                           help='dump event log',
                           type=bool)
    tornado.options.define('timescale', default=1.0,
                           help='accelerate time by this factor',
                           type=float)
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 2:
        options.filename = remaining[0]
        options.server_urls = remaining[1:]
    else:
        parser.error('At least one file name and one server URL required')
    return options

def main():
    options = read_cmd_options()
    entity_id = streamsem.random_id()
    publishers = [client.EventPublisher(url) for url in options.server_urls]
    io_loop = tornado.ioloop.IOLoop.instance()
    scheduler = EventScheduler(options.filename, io_loop, publishers,
                               tornado.options.options.timescale,
                               compressed=True)
    if tornado.options.options.eventlog:
        logger.logger = logger.StreamsemLogger(entity_id,
                                               'replay-' + entity_id + '.log')
    try:
        io_loop.start()
    except KeyboardInterrupt:
        pass
    finally:
        logger.logger.close()

if __name__ == "__main__":
    main()
