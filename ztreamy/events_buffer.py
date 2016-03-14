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

import os
import os.path
import shutil
import base64
import re

import tornado.concurrent
import concurrent.futures

from . import events


class PendingEventsBuffer(object):
    """Buffer for the events waiting to be dispatched."""
    def __init__(self):
        self.events = []
        self.event_ids = set()

    def get_events(self, reset=True):
        """Get the events waiting and, by default, reset this buffer."""
        events = self.events
        self.reset()
        return events

    def reset(self):
        """Reset this buffer."""
        self.events = []
        self.event_ids = set()

    def add_event(self, event):
        """Add an event to this buffer.

        Returns True if the event is accepted, False otherwise.
        The event won't be accepted if there is another one with the
        same event_id.

        An event shouldn't be modified after it has been added to this buffer
        because this is the version of the event that will be stored in disk
        in subclasses that provide persistence.

        """
        if not event.event_id in self.event_ids:
            self.events.append(event)
            self.event_ids.add(event.event_id)
            accepted = True
        else:
            accepted = False
        return accepted

    def add_events(self, events):
        """Add a list of events to this buffer.

        Returns a new list with the events that were accepted.
        An event won't be accepted if there is another one with the
        same event_id.

        An event shouldn't be modified after it has been added to this buffer
        because this is the version of the event that will be stored in disk
        in subclasses that provide persistence.

        """
        accepted = []
        for event in events:
            if self.add_event(event):
                accepted.append(event)
        return accepted

    def is_duplicate(self, event):
        """True if there is an event with the same event_id in the buffer."""
        return event.event_id in self.event_ids


class PersistentPendingEventsBuffer(PendingEventsBuffer):
    """A pending event buffer that backs the events up to disk."""

    DIRNAME_STORE = 'pending'
    DIRNAME_CURRENT = 'current'
    DIRNAME_PREVIOUS = 'previous'

    def __init__(self, stream_label, reset=False, base_dir=''):
        super(PersistentPendingEventsBuffer, self).__init__()
        self.store_dir = os.path.join(base_dir,
                                      '.ztreamy-stream-' + stream_label,
                                      self.DIRNAME_STORE)
        self.current_dir = os.path.join(self.store_dir, self.DIRNAME_CURRENT)
        self.previous_dir = os.path.join(self.store_dir, self.DIRNAME_PREVIOUS)
        self._init_store(reset)

    def add_event(self, event):
        """Add an event to this buffer andback it up to disk.

        Returns True if the event is accepted, False otherwise.
        The event won't be accepted if there is another one with the
        same event_id.

        An event shouldn't be modified after it has been added to this buffer
        because this is the version of the event that will be stored in disk.

        """
        accepted = super(PersistentPendingEventsBuffer, self).add_event(event)
        if accepted:
            self._store_event(event, len(self.events) - 1)
        return accepted

    def reset(self):
        """Reset this buffer."""
        super(PersistentPendingEventsBuffer, self).reset()
        self._roll_current_dir()

    def move_event_file(self, index, event_id, dest_filename):
        orig_filename = os.path.join(self.previous_dir,
                                     self._filename(index, event_id))
        try:
            os.rename(orig_filename, dest_filename)
        except IOError:
            if os.path.exists(orig_filename):
                # In Windows an IOError is raised if the destination exists
                os.remove(dest_filename)
                os.rename(orig_filename, dest_filename)
            else:
                raise

    def _init_store(self, reset):
        if reset and os.path.exists(self.store_dir):
            shutil.rmtree(self.store_dir)
        if not os.path.exists(self.store_dir):
            self._create_store()
        else:
            self._read_store()

    def _create_store(self):
        # We assume that the in-memory buffer is already initialized and empty
        # The self.store_dir directory gets created automatically
        # because current_dir is its subdirectory
        os.makedirs(self.current_dir)

    def _read_store(self):
        for event in self._read_events():
            super(PersistentPendingEventsBuffer, self).add_event(event)

    def _read_events(self):
        files = self._files_in_store()
        for index, event_id, filename in files:
            try:
                event = events.single_event_from_file(filename)
            except IOError:
                event = None
            if event is not None and event.event_id == event_id:
                yield event
            else:
                os.remove(filename)

    _re_filename = re.compile(r'(\d+)-(.*)')

    def _files_in_store(self):
        files = []
        for name in os.listdir(self.current_dir):
            match = PersistentPendingEventsBuffer._re_filename.search(name)
            if match:
                index, encoded_event_id = match.groups()
                event_id = base64.urlsafe_b64decode(str(encoded_event_id))
                full_name = os.path.join(self.current_dir, name)
                files.append((int(index), event_id, full_name))
        return sorted(files)

    def _store_event(self, event, index):
        name = self._filename(index, event.event_id)
        with open(os.path.join(self.current_dir, name), mode='w') as f:
            f.write(str(event))

    def _filename(self, index, event_id):
        return '{}-{}'.format(index, base64.urlsafe_b64encode(event_id))

    def _roll_current_dir(self):
        if os.path.exists(self.previous_dir):
            shutil.rmtree(self.previous_dir)
        os.rename(self.current_dir, self.previous_dir)
        os.makedirs(self.current_dir)


class PendingEventsBufferAsync(PersistentPendingEventsBuffer):
    """A pending event buffer that backs the events up to disk.

    The I/O operations are done on another thread with an executor,
    so that the server does not block.

    """
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def __init__(self, stream_label, io_loop, **kwargs):
        self.io_loop = io_loop
        super(PendingEventsBufferAsync, self).__init__(stream_label, **kwargs)

    @tornado.concurrent.run_on_executor
    def move_event_file(self, index, event_id, dest_filename):
        super(PendingEventsBufferAsync, self).move_event_file( \
                                                index, event_id, dest_filename)

    @tornado.concurrent.run_on_executor
    def _store_event(self, event, index):
        super(PendingEventsBufferAsync, self)._store_event(event, index)

    @tornado.concurrent.run_on_executor
    def _roll_current_dir(self):
        super(PendingEventsBufferAsync, self)._roll_current_dir()


class EventsBuffer(object):
    """A circular buffer that stores the latest events of a stream."""
    def __init__(self, size):
        """Creates a new buffer with capacity for 'size' events."""
        self.buffer = [None] * size
        self.position = 0
        self.events = {}

    @property
    def size(self):
        return len(self.buffer)

    def append_event(self, event):
        """Appends an event to the buffer."""
        if self.buffer[self.position] is not None:
            del self.events[self.buffer[self.position].event_id]
        self.buffer[self.position] = event
        self.events[event.event_id] = self.position
        self.position += 1
        if self.position == self.size:
            self.position = 0

    def append_events(self, events):
        """Appends a list of events to the buffer."""
        if len(events) > self.size:
            events = events[-self.size:]
        if self.position + len(events) >= self.size:
            first_block = self.size - self.position
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
            if pos == self.size:
                pos = 0
            if pos == self.position:
                data = []
            elif pos < self.position:
                data = self.buffer[pos:self.position]
            else:
                data = self.buffer[pos:] + self.buffer[:self.position]
        else:
            complete = False
            if (self.position != self.size - 1
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
        if num_events > self.size:
            num_events = self.size
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
        self.position = (self.position + len(events)) % self.size

    def _remove_from_dict(self, position, num_events):
        for i in range(position, position + num_events):
            if self.buffer[i] is not None:
                if self.buffer[i].event_id in self.events:
                    del self.events[self.buffer[i].event_id]


class PersistentEventsBuffer(EventsBuffer):
    """Events buffer for recent events that backs up to disk.

    The buffer is automatically reloaded from disk upon restart.

    """
    FILENAME_POSITION = 'position'
    FILENAME_SIZE = 'size'
    DIRNAME_STORE = 'events_buffer'

    def __init__(self, size, stream_label, reset=False, base_dir=''):
        super(PersistentEventsBuffer, self).__init__(size)
        self.store_dir = os.path.join(base_dir,
                                      '.ztreamy-stream-' + stream_label,
                                      self.DIRNAME_STORE)
        self._size = size
        self.store_pos_filename = None
        self.store_size_filename = None
        self._init_store(reset)

    @property
    def size(self):
        return self._size

    def append_event(self, event):
        initial_pos = self.position
        super(PersistentEventsBuffer, self).append_event(event)
        self._store_event(initial_pos, event)
        self._store_position()

    def append_events(self, events):
        initial_pos = self.position
        super(PersistentEventsBuffer, self).append_events(events)
        if initial_pos + len(events) >= self.size:
            if len(events) <= self.size:
                offset = 0
            else:
                offset = len(events) - self.size
            first_block = self.size - initial_pos
            self._store_events(initial_pos, events,
                               offset, offset + first_block)
            self._store_events(0, events,
                               offset + first_block, len(events))
        else:
            self._store_events(initial_pos, events, 0, len(events))
        self._store_position()

    def _init_store(self, reset):
        self.store_pos_filename = os.path.join(self.store_dir,
                                               self.FILENAME_POSITION)
        self.store_size_filename = os.path.join(self.store_dir,
                                                self.FILENAME_SIZE)
        if reset and os.path.exists(self.store_dir):
            shutil.rmtree(self.store_dir)
        if not os.path.exists(self.store_dir):
            self._create_store()
        else:
            self._read_store()

    def _create_store(self):
        # We assume that the in-memory buffer is already initialized and empty
        os.makedirs(self.store_dir)
        self._store_number(self.store_size_filename, self.size)
        self._store_position()

    def _read_store(self):
        size = self._read_number(self.store_size_filename)
        if size != self.size:
            raise ValueError('Events buffer size is different in disk')
        self.position = self._read_number(self.store_pos_filename)
        if self.position < 0 or self.position >= self.size:
            raise ValueError('Wrong position number in events buffer')
        for i in range(self.size):
            event = self._read_event(i)
            if event is not None:
                self.buffer[i] = event
                self.events[event.event_id] = i
            elif i != self.position:
                # The buffer is not full but we aren't
                # at the current position as we should
                raise ValueError('Wrong buffer position')
            else:
                # The buffer is not full but everything is ok
                # because we are at the right position
                break

    def _store_position(self):
        self._store_number(self.store_pos_filename, self.position)

    def _read_event(self, index):
        try:
            event = events.single_event_from_file( \
                                    os.path.join(self.store_dir, str(index)))
        except IOError:
            event = None
        return event

    def _store_event(self, index, event):
        with open(os.path.join(self.store_dir, str(index)), mode='w') as f:
            f.write(str(event))

    def _store_events(self, ini_index, events, ini, end):
        offset = ini_index - ini
        for i in range(ini, end):
            self._store_event(offset + i, events[i])

    @staticmethod
    def _store_number(filename, number):
        with open(filename, mode='w') as f:
            f.write(str(number))

    @staticmethod
    def _read_number(filename):
        with open(filename) as f:
            number = int(f.readline().strip())
        return number


class CoordinatedEventsBuffer(PersistentEventsBuffer):
    """Events buffer for recent events that backs up to disk.

    This buffer coordinates with a persistent pending events buffer
    in order to avoid saving each event twice. They are moved from
    the store of the pending events buffer to this store when possible.

    The buffer is automatically reloaded from disk upon restart.

    """
    def __init__(self, size, stream_label, pending_buffer, **kwargs):
        super(CoordinatedEventsBuffer, self).__init__(size, stream_label,
                                                      **kwargs)
        self.pending_buffer = pending_buffer

    def _store_events(self, ini_index, events, ini, end):
        offset = ini_index - ini
        for i in range(ini, end):
            try:
                self.pending_buffer.move_event_file(i, events[i].event_id,
                                os.path.join(self.store_dir, str(offset + i)))
            except IOError:
                self._store_event(offset + i, events[i])


class EventsBufferAsync(CoordinatedEventsBuffer):
    """Events buffer for recent events that backs up to disk.

    This buffer coordinates with a persistent pending events buffer
    in order to avoid saving each event twice. They are moved from
    the store of the pending events buffer to this store when possible.

    The buffer is automatically reloaded from disk upon restart.

    """
    def __init__(self, size, stream_label, pending_buffer, **kwargs):
        self.executor = pending_buffer.__class__.executor
        self.io_loop = pending_buffer.io_loop
        super(EventsBufferAsync, self).__init__(size, stream_label,
                                                pending_buffer,
                                                **kwargs)

    @tornado.concurrent.run_on_executor
    def _store_event(self, index, event):
        super(EventsBufferAsync, self)._store_event(index, event)

    @tornado.concurrent.run_on_executor
    def _store_number(self, filename, number):
        super(EventsBufferAsync, self)._store_number(filename, number)
