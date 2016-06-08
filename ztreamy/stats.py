# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2016 Jesus Arias Fisteus
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
""" Code for keeping statistics such as event counts

"""
from __future__ import print_function


class StreamStats(object):
    """Keeps track of a stram's stats.

    Right now, it only counts events, but it could do more in the future.

    """
    def __init__(self):
        self._num_events = 0

    @property
    def num_events(self):
        """Return the total number of events published by the stream."""
        return self._num_events

    def count_events(self, num):
        """Count `num` events more as published in the stream.

        This method should be called once the events have been
        actually published (not while they are still buffered).
        Parameter `num` should be a non-negative integer number.

        """
        self._num_events += num
