import tornado.ioloop
import tornado.options
import logging

import streamsem
from streamsem import events
from streamsem import rdfevents
from streamsem import client
from streamsem import logger
from streamsem.tools import utils

class Scheduler(utils.EventScheduler):
    def __init__(self, num_events, source_id, io_loop, publishers,
                 time_generator=None, add_timestamp=False):
        generator = self._create_events(num_events, source_id)
        super(Scheduler, self).__init__(source_id, io_loop, publishers, 1.0,
                                        time_generator=time_generator,
                                        add_timestamp=add_timestamp,
                                        event_generator=generator)

    def _create_events(self, num_events, source_id):
        last_sequence_num = 0
        while num_events <= 0 or num_events > last_sequence_num:
            last_sequence_num += 1
            yield events.TestEvent(source_id, 'streamsem-test', None,
                                   sequence_num=last_sequence_num)


def read_cmd_options():
    from optparse import Values
    tornado.options.define('distribution', default='exp(5)',
                           help='distribution of the time between events')
    tornado.options.define('limit', default=0, type=int,
                           help='number of events to generate')
    tornado.options.define('eventlog', default=False,
                           help='dump event log',
                           type=bool)
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 1:
        options.server_urls = remaining
    else:
        parser.error('At least one server URL required')
    return options

def main():
    options = read_cmd_options()
    entity_id = streamsem.random_id()
    limit = tornado.options.options.limit
    publishers = [client.EventPublisher(url) for url in options.server_urls]
    io_loop = tornado.ioloop.IOLoop.instance()
    time_generator = utils.get_scheduler(tornado.options.options.distribution)
    scheduler = Scheduler(limit, entity_id, io_loop, publishers,
                          time_generator=time_generator, add_timestamp=True)
    if tornado.options.options.eventlog:
        logger.logger = logger.StreamsemLogger(entity_id,
                                               'source-' + entity_id + '.log')
    try:
        io_loop.start()
    except KeyboardInterrupt:
        pass
    finally:
        logger.logger.close()

if __name__ == "__main__":
    main()
