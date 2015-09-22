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

def compression_ratio(filename):
    comp_size = 0
    raw_size = 0
    ratios = []
    with open(filename, 'r') as file_:
        for line in file_:
            if line.startswith('#'):
                continue
            data = [s.strip() for s in line.split('\t')]
            if data[0] == 'data_receive':
                comp_size += int(data[1])
                raw_size += int(data[2])
                if raw_size > 0:
                    ratios.append((raw_size, float(comp_size) / raw_size))
    return ratios

def read_cmd_options():
    parser = OptionParser(usage = 'usage: %prog [options] <log_filename>')
    (options, args) = parser.parse_args()
    if len(args) == 1:
        options.filename = args[0]
    else:
        parser.error('Log filename expected')
    return options

def main():
    options = read_cmd_options()
    data = compression_ratio(options.filename)
    print('\n'.join(['%d\t%.6f'%(t, r) for t, r in data]))

if __name__ == '__main__':
    main()
