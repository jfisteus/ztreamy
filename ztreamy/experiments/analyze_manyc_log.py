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

from optparse import OptionParser

from ztreamy.tools import utils

class DelayStats(object):
    def __init__(self, event_num, delays):
        self.event_num = event_num
        self.delays = delays

    def __str__(self):
        return '\t'.join((str(self.event_num), str(self.average),
                          str(self.std_dev), str(self.median),
                          str(self.minimum), str(self.maximum)))

    def append_delays(self, new_delays):
        self.delays.extend(new_delays)

    def analyze(self):
        self.average, self.std_dev = utils.average_and_std_dev(self.delays)
        self.median = utils.median(self.delays)
        self.minimum = min(self.delays)
        self.maximum = max(self.delays)

def process_file(filename, stats):
    with open(filename, 'r') as file_:
        for line in file_:
            if line.startswith('#'):
                continue
            data = [s.strip() for s in line.split('\t')]
            if data[0] == 'manyc_event_finish':
                event_num = int(data[1])
                delays = [float(d) for d in data[2:]]
                stats[-1].append_delays(delays)
                if event_num in stats:
                    stats[event_num].append_delays(delays)
                else:
                    stats[event_num] = DelayStats(event_num, delays)

def manyc_delays(filenames):
    stats = {}
    stats[-1] = DelayStats(-1, [])
    for filename in filenames:
        process_file(filename, stats)
    for entry in stats.itervalues():
        entry.analyze()
    return stats

def read_cmd_options():
    parser = OptionParser(usage = 'usage: %prog [options] <log_filename>')
    (options, args) = parser.parse_args()
    if len(args) >= 1:
        options.filenames = args
    else:
        parser.error('At least one log filename expected')
    return options

def main():
    options = read_cmd_options()
    stats = manyc_delays(options.filenames)
    print('\n'.join([str(stats[num]) for num in sorted(stats.keys())]))

if __name__ == '__main__':
    main()
