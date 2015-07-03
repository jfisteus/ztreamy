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
from __future__ import division

import gzip

from ztreamy import Deserializer

def count_events(filename):
    """Counts the number of events in a file and their total size.

    Returns a tuple (num_events, num_bytes).

    """
    num_bytes = 0
    num_events = 0
    if filename.endswith('.gz'):
        file_ = gzip.GzipFile(filename, 'r')
    else:
        file_ = open(filename, 'r')
    deserializer = Deserializer()
    while True:
        data = file_.read(1024)
        if data == '':
            break
        evs = deserializer.deserialize(data, parse_body=False, complete=False)
        num_bytes += len(data)
        num_events += len(evs)
    file_.close()
    return num_events, num_bytes

def main():
    import sys
    num_events, num_bytes = count_events(sys.argv[1])
    print('Number of bytes: {0}'.format(num_bytes))
    print('Number of events: {0}'.format(num_events))
    print('Average event size : {0:.2f}'.format(num_bytes / num_events))

if __name__ == "__main__":
    main()
