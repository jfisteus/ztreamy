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
"""Analyzes log files to obtain performance statistics.

"""
from __future__ import print_function

import sys

class Analyzer(object):
    def __init__(self):
        self.events = {}
        self.data_delivery_log = DataDeliveryLog()

    def register_publish(self, event_id, publish_time):
        event = self._get_or_create(event_id)
        event.publish_times.append(publish_time)

    def register_dispatch(self, event_id, dispatch_time):
        event = self._get_or_create(event_id)
        event.dispatch_times.append(dispatch_time)

    def register_deliver(self, event_id, deliver_time):
        event = self._get_or_create(event_id)
        event.deliver_times.append(deliver_time)

    def register_data_delivery(self, compressed, uncompressed):
        self.data_delivery_log.register_data_delivery(compressed, uncompressed)

    def analyze(self):
        self.delivery_delays = []
        for key, event in self.events.iteritems():
            event.analyze()
            self.delivery_delays.extend(event.delivery_delays)

    def parse_file(self, filename):
        with open(filename) as file_:
            for line in file_:
                line = line.strip()
                if line.startswith('#') or line == '':
                    continue
                parts = [part.strip() for part in line.split('\t')]
                if parts[0] == 'event_publish':
                    assert len(parts) == 3
                    self.register_publish(parts[1], float(parts[2]))
                elif parts[0] == 'event_dispatch':
                    assert len(parts) == 3
                    self.register_dispatch(parts[1], float(parts[2]))
                elif parts[0] == 'event_deliver':
                    assert len(parts) == 3
                    self.register_deliver(parts[1], float(parts[2]))
                elif parts[0] == 'data_receive':
                    assert len(parts) == 3
                    self.register_data_delivery(int(parts[1]), int(parts[2]))

    def _get_or_create(self, event_id):
        if not event_id in self.events:
            self.events[event_id] = EventLog(event_id)
        return self.events[event_id]


class EventLog(object):
    def __init__(self, event_id):
        self.event_id = event_id
        self.publish_times = []
        self.dispatch_times = []
        self.deliver_times = []

    def register_publish(self, publish_time):
        self.publish_times.append(publish_time)

    def register_dispatch(self, dispatch_time):
        self.dispatch_times.append(dispatch_time)

    def register_deliver(self, deliver_time):
        self.deliver_times.append(deliver_time)

    def analyze(self):
        self.publish_times.sort()
        self.dispatch_times.sort()
        self.delivery_delays = [deliver - self.publish_times[0] \
                                    for deliver in self.deliver_times]

    def __str__(self):
        parts = ['publish: %s'%str(self.publish_times),
                 'dispatch: %s'%str(self.dispatch_times),
                 'deliver: %s'%str(self.deliver_times)]
        return '\n'.join(parts)


class DataDeliveryLog(object):
    def __init__(self):
        self.uncompressed_data = 0
        self.compressed_data = 0

    def register_data_delivery(self, compressed, uncompressed):
        self.compressed_data += compressed
        self.uncompressed_data += uncompressed


def median(data):
    data = sorted(data)
    n = len(data)
    if n % 2 == 1:
        return data[n // 2]
    else:
        return float(data[(n - 1) // 2] + data[n // 2]) / 2

def main():
    logfiles = sys.argv[1:]
    analyzer = Analyzer()
    for logfile in logfiles:
        analyzer.parse_file(logfile)
    analyzer.analyze()
    min_delay = min(analyzer.delivery_delays)
    max_delay = max(analyzer.delivery_delays)
    avg_delay = sum(analyzer.delivery_delays) / len(analyzer.delivery_delays)
    med_delay = median(analyzer.delivery_delays)
    print('Delays: {} {} {} {}'.format(min_delay, avg_delay, max_delay, med_delay))
    data_log = analyzer.data_delivery_log
    print('Data: {} {} {}'.format(data_log.uncompressed_data, data_log.compressed_data, \
        float(data_log.compressed_data) / data_log.uncompressed_data))

if __name__ == "__main__":
    main()
