import tornado.ioloop
import tornado.options
import logging

import streamsem
from streamsem import events
from streamsem import rdfevents
from streamsem import logger
from streamsem import client

class Processor(object):
    def __init__(self, stream_urls):
        self.client = client.Client(stream_urls,
                                    event_callback=self._handle_event,
                                    error_callback=self._handle_error)

    def start(self, loop=False):
        self.client.start(loop=loop)

    def stop(self):
        self.client.stop()

    def _handle_error(self, message, http_error=None):
        pass

    def _handle_event(self, event):
        print event


def read_cmd_options():
    from optparse import OptionParser, Values
    tornado.options.define('eventlog', default=False,
                           help='dump event log',
                           type=bool)
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 1:
        options.stream_urls = remaining
    else:
        parser.error('At least one source stream URL required')
    return options

def main():
    options = read_cmd_options()
#    import streamsem.filters
#    filter = streamsem.filters.SimpleTripleFilter(handle_event,
#                                        predicate='http://example.com/temp')
    if tornado.options.options.eventlog:
        logger.logger = logger.StreamsemLogger(node_id,
                                               'processor-' + node_id + '.log')
    processor = Processor(options.stream_urls)
    try:
        processor.start(loop=True)
    except KeyboardInterrupt:
        pass
    finally:
        processor.stop()
        logger.logger.close()

if __name__ == "__main__":
    main()
