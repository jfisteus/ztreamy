import tornado.ioloop
import tornado.options
from rdflib.graph import Graph

import streamsem
from streamsem import logger
from streamsem import client

class Processor(object):
    def __init__(self, stream_urls):
        self.client = client.Client(stream_urls,
                                    event_callback=self._handle_event,
                                    error_callback=self._handle_error,
                                    separate_events=False)
        self.triple_store = Graph('Sleepycat',
                                  'http://www.it.uc3m.es/jaf/ns/slog/db')
        self.db_dir = 'dbdir'

    def start(self, loop=False):
        self.triple_store.open(self.db_dir)
        self.client.start(loop=loop)

    def stop(self):
        self.triple_store.close()
        self.client.stop()

    def _handle_error(self, message, http_error=None):
        pass

    def _handle_event(self, evs):
        print 'Received %d events.'%len(evs)
        for event in evs:
            self.triple_store += event.body
        print len(self.triple_store)


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
        OptionParser.error('At least one source stream URL required')
    return options

def main():
    options = read_cmd_options()
    node_id = streamsem.random_id()
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
