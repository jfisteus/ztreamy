# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2015 Jesus Arias Fisteus
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
#
from __future__ import print_function

import tornado.ioloop
import tornado.options
from rdflib.graph import Graph

import ztreamy
from ztreamy import logger
from ztreamy import client

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
        print('Received {} events.'.format(len(evs)))
        for event in evs:
            self.triple_store += event.body
        print(len(self.triple_store))


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
    node_id = ztreamy.random_id()
#    import ztreamy.filters
#    filter = ztreamy.filters.SimpleTripleFilter(handle_event,
#                                        predicate='http://example.com/temp')
    if tornado.options.options.eventlog:
        logger.logger = logger.ZtreamyLogger(node_id,
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
