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

import tornado.options

from ztreamy import RelayStream, StreamServer, logger


def read_cmd_options():
    from optparse import Values, OptionParser
    tornado.options.define('port', default=8888, help='run on the given port',
                           type=int)
    tornado.options.define('aggregatorid', default=None,
                           help='aggregator id', type=str)
    tornado.options.define('buffer', default=None, help='event buffer time (s)',
                           type=float)
    tornado.options.define('eventlog', default=False,
                           help='dump event log',
                           type=bool)
    tornado.options.define('autostop', default=False,
                           help='stop the server when the source finishes',
                           type=bool)
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 1:
        options.stream_urls = remaining
    else:
        OptionParser().error('At least one source stream URL required')
    return options

def main():
    options = read_cmd_options()
    def stop_server():
        server.stop()
#    import ztreamy.filters
#    filter_=ztreamy.filters.SourceFilter(None,
#                             source_id='65f0bfeb-cc79-4188-8404-175f3a6be6c3')
    if (tornado.options.options.buffer is not None
        and tornado.options.options.buffer > 0):
        buffering_time = tornado.options.options.buffer * 1000
    else:
        buffering_time = None
    server = StreamServer(tornado.options.options.port,
                stop_when_source_finishes=tornado.options.options.autostop)
    stream = RelayStream('/relay', options.stream_urls,
                buffering_time=buffering_time,
                stop_when_source_finishes=tornado.options.options.autostop)
    server.add_stream(stream)

    # Uncomment to test RelayStream.stop():
#    import time
#    tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 5, stop_server)

    if tornado.options.options.eventlog:
        print(stream.source_id)
        comments = {'Buffer time (ms)': buffering_time}
#        logger.logger = logger.ZtreamyLogger(stream.source_id,
        logger.logger = logger.CompactServerLogger(stream.source_id,
                                                   'relay-' + stream.source_id
                                                   + '.log', comments)
        logger.logger.auto_flush = True

    try:
        server.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
        logger.logger.close()


if __name__ == "__main__":
    main()
