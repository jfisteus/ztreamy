import tornado.ioloop
import tornado.options
import logging

import streamsem
from streamsem import events
from streamsem import rdfevents
from streamsem import client
from streamsem import logger
from streamsem.tools import utils

def read_cmd_options():
    from optparse import Values
    tornado.options.define('distribution', default='exp(5)',
                           help='distribution of the time between events')
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
    def schedule_next_event():
        io_loop.add_timeout(scheduler.next(), publish_event)
    def publish_event():
        logging.info('In publish_event')
        num_events_created[0] += 1
        schedule_next_event()
        event = events.TestEvent(source_id, 'streamsem-test', None,
                                 sequence_num=num_events_created[0])
        for p in publishers:
            p.publish(event)
    def finish():
        for p in publishers:
            p.close()
        tornado.ioloop.IOLoop.instance().stop()
    # the list is a trick to make the variable writable from publish_event
    num_events_created = [0]
    options = read_cmd_options()
    publishers = [client.EventPublisher(url) for url in options.server_urls]
    io_loop = tornado.ioloop.IOLoop.instance()
    scheduler = utils.get_scheduler(tornado.options.options.distribution)
    for i in range(0, 10):
        schedule_next_event()
    application_id = '1111-1111'
    source_id = streamsem.random_id()
    if tornado.options.options.eventlog:
        logger.logger = logger.StreamsemLogger(source_id,
                                               'source-' + source_id + '.log')
    try:
        io_loop.start()
    except KeyboardInterrupt:
        pass
    finally:
        logger.logger.close()

if __name__ == "__main__":
    main()
