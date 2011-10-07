import tornado.ioloop
import tornado.httpclient
import tornado.options
import logging
import time
import numpy

import streamsem
from streamsem import events
from streamsem import rdfevents
from streamsem import client
from streamsem import logger

def read_cmd_options():
    from optparse import OptionParser, Values
    parser = OptionParser(usage='usage: %prog [options] server_urls',
                          version='0.0')
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 1:
        options.server_urls = remaining
    else:
        parser.error('At least one server URL required')
    return options

def events_scheduler():
    last = time.time()
    while True:
        last += numpy.random.exponential(2)
        yield last

def main():
    def schedule_next_event():
        io_loop.add_timeout(scheduler.next(), publish_event)
    def publish_event():
        logging.info('In publish_event')
        schedule_next_event()
        event_body = ('<http://example.com/now> '
                       '<http://example.com/time> "%s".'%time.time())
        event = rdfevents.RDFEvent(source_id, 'n3', event_body,
                                   application_id=application_id)
        for p in publishers:
            p.publish(event)
    def publish_event2():
        logging.info('In publish_event2')
        event_body = ('<http://example.com/now> '
                       '<http://example.com/temp> "%s".'%25)
        event = rdfevents.RDFEvent(source_id, 'n3', event_body,
                                   application_id=application_id)
        for p in publishers:
            p.publish(event)
    def finish():
        for p in publishers:
            p.close()
        tornado.ioloop.IOLoop.instance().stop()

    options = read_cmd_options()
    publishers = [client.EventPublisher(url) for url in options.server_urls]
    io_loop = tornado.ioloop.IOLoop.instance()
    scheduler = events_scheduler()
    for i in range(0, 10):
        schedule_next_event()
    application_id = '1111-1111'
    source_id = streamsem.random_id()
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
