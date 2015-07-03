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
import tornado.ioloop
import tornado.options

import ztreamy
from ztreamy import events
from ztreamy import client
from ztreamy import logger
from ztreamy.tools import utils

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
            yield events.TestEvent(source_id, 'ztreamy-test', None,
                                   sequence_num=last_sequence_num)


def read_cmd_options():
    from optparse import OptionParser, Values
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
        OptionParser().error('At least one server URL required')
    return options

def main():
    options = read_cmd_options()
    entity_id = ztreamy.random_id()
    limit = tornado.options.options.limit
    publishers = [client.EventPublisher(url) for url in options.server_urls]
    io_loop = tornado.ioloop.IOLoop.instance()
    time_generator = utils.get_scheduler(tornado.options.options.distribution)
    scheduler = Scheduler(limit, entity_id, io_loop, publishers,
                          time_generator=time_generator, add_timestamp=True)
    if tornado.options.options.eventlog:
        logger.logger = logger.ZtreamyLogger(entity_id,
                                             'source-' + entity_id + '.log')
    try:
        io_loop.start()
    except KeyboardInterrupt:
        pass
    finally:
        logger.logger.close()

if __name__ == "__main__":
    main()
