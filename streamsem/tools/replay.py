# streamsem: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2012 Jesus Arias Fisteus
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
# <http://www.gnu.org/
#
import gzip
import tornado.options
import tornado.ioloop

import streamsem
import streamsem.client as client
import streamsem.events as events
import streamsem.logger as logger
from streamsem.tools import utils

class RelayScheduler(utils.EventScheduler):
    def __init__(self, filename, num_events, source_id, io_loop, publishers,
                 time_scale, time_generator=None, add_timestamp=False):
        generator = self._read_event_file(filename, num_events)
        super(RelayScheduler, self).__init__(source_id, io_loop, publishers,
                                             time_scale,
                                             time_generator=time_generator,
                                             add_timestamp=add_timestamp,
                                             event_generator=generator)

    def _read_event_file(self, filename, num_events):
        last_sequence_num = 0
        if filename.endswith('.gz'):
            file_ = gzip.GzipFile(filename, 'r')
        else:
            file_ = open(filename, 'r')
        deserializer = events.Deserializer()
        while num_events <= 0 or num_events > last_sequence_num:
            data = file_.read(1024)
            if data == '':
                break
            evs = deserializer.deserialize(data, parse_body=False,
                                           complete=False)
            for event in evs:
                if num_events > 0 and num_events <= last_sequence_num:
                    break
                last_sequence_num += 1
                if self.add_timestamp:
                    # The timestamp header is set later, just before sending
                    event.sequence_num = last_sequence_num
                yield event
        file_.close()


def read_cmd_options():
    from optparse import OptionParser, Values
    tornado.options.define('distribution', default=None,
                           help='distribution of the time between events')
    tornado.options.define('limit', default=0, type=int,
                           help='number of events to generate')
    tornado.options.define('eventlog', default=False,
                           help='dump event log',
                           type=bool)
    tornado.options.define('timestamp', default=False,
                           help='add an X-Float-Timestamp header to events',
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
        OptionParser().error('At least one file name and one '
                             'server URL required')
    return options

def main():
    options = read_cmd_options()
    entity_id = streamsem.random_id()
    limit = tornado.options.options.limit
    publishers = [client.EventPublisher(url) for url in options.server_urls]
    io_loop = tornado.ioloop.IOLoop.instance()
    if tornado.options.options.distribution is not None:
        time_generator = \
            utils.get_scheduler(tornado.options.options.distribution)
    else:
        time_generator = None
    scheduler = RelayScheduler(options.filename, limit, entity_id, io_loop,
                               publishers, tornado.options.options.timescale,
                               time_generator=time_generator,
                               add_timestamp=tornado.options.options.timestamp)
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
