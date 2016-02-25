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
""" Implementation of the recent events buffer used by streams.

There is a RAM-only buffer and a persistent buffer that backs
the events up to disk, so that they can be loaded from there
in the next startup of the stream.

"""
from __future__ import print_function

from . import events


class RecentEventsBuffer(object):
    """A circular buffer that stores the latest events of a stream."""
    def __init__(self, size):
        """Creates a new buffer with capacity for 'size' events."""
        self.buffer = [None] * size
        self.position = 0
        self.events = {}

    def append_event(self, event):
        """Appends an event to the buffer."""
        if self.buffer[self.position] is not None:
            del self.events[self.buffer[self.position].event_id]
        self.buffer[self.position] = event
        self.events[event.event_id] = self.position
        self.position += 1
        if self.position == len(self.buffer):
            self.position = 0

    def append_events(self, events):
        """Appends a list of events to the buffer."""
        if len(events) > len(self.buffer):
            events = events[-len(self.buffer):]
        if self.position + len(events) >= len(self.buffer):
            first_block = len(self.buffer) - self.position
            self._append_internal(events[:first_block])
            self._append_internal(events[first_block:])
        else:
            self._append_internal(events)

    def load_from_file(self, file_):
        deserializer = events.Deserializer()
        for evs in deserializer.deserialize_file(file_):
            self.append_events(evs)

    def newer_than(self, event_id, limit=None):
        """Returns the events newer than the given 'event_id'.

        If no event in the buffer has the 'event_id', all the events are
        returned. If the newest event in the buffer has that id, an empty
        list is returned.

        If 'limit' is not None, at most 'limit' events are returned
        (the most recent ones).

        Returns a tuple ('events', 'complete') where 'events' is the list
        of events and 'complete' is True when 'event_id' is in the buffer
        and no limit was applied.

        """
        if event_id in self.events:
            complete = True
            pos = self.events[event_id] + 1
            if pos == len(self.buffer):
                pos = 0
            if pos == self.position:
                data = []
            elif pos < self.position:
                data = self.buffer[pos:self.position]
            else:
                data = self.buffer[pos:] + self.buffer[:self.position]
        else:
            complete = False
            if (self.position != len(self.buffer) - 1
                and self.buffer[self.position + 1] is None):
                data = self.buffer[:self.position]
            else:
                data = (self.buffer[self.position:]
                        + self.buffer[:self.position])
        if limit is not None and len(data) > limit:
            data = data[-limit:]
            complete = False
        return data, complete

    def most_recent(self, num_events):
        if num_events > len(self.buffer):
            num_events = len(self.buffer)
        pos = self.position - num_events
        if pos >= 0:
            data = self.buffer[pos:self.position]
        elif self.buffer[-1] is not None:
            data = self.buffer[pos:] + self.buffer[:self.position]
        else:
            data = self.buffer[:self.position]
        return data

    def contains(self, event):
        return event.event_id in self.events

    def _append_internal(self, events):
        self._remove_from_dict(self.position, len(events))
        self.buffer[self.position:self.position + len(events)] = events
        for i in range(0, len(events)):
            self.events[events[i].event_id] = self.position + i
        self.position = (self.position + len(events)) % len(self.buffer)

    def _remove_from_dict(self, position, num_events):
        for i in range(position, position + num_events):
            if self.buffer[i] is not None:
                if self.buffer[i].event_id in self.events:
                    del self.events[self.buffer[i].event_id]
