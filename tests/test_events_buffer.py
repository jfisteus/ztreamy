# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2014-2015 Jesus Arias Fisteus
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

import unittest
import tempfile
import shutil

from ztreamy.events_buffer import EventsBuffer, PersistentEventsBuffer
from ztreamy import events


class TestEventBuffer(unittest.TestCase):

    def setUp(self):
        self.events = [_MockEvent('xxxx-xxxx-xx{:02d}'.format(i)) \
                       for i in range(100)]

    def test_buffer_complete_no_overflow(self):
        buf = _EventsBufferStrict(8)
        buf.append_event(self.events[0])
        buf.append_events(self.events[1:6])
        buf.append_event(self.events[6])
        data, complete = buf.newer_than('xxxx-xxxx-xx03')
        self.assertTrue(complete)
        self.assertEqual(data, self.events[4:7])
        self.assertEqual(len(buf.events), 7)

    def test_buffer_complete_overflow(self):
        buf = _EventsBufferStrict(8)
        buf.append_event(self.events[0])
        buf.append_events(self.events[1:6])
        buf.append_event(self.events[6])
        buf.append_events(self.events[7:10])
        data, complete = buf.newer_than('xxxx-xxxx-xx04')
        self.assertTrue(complete)
        self.assertEqual(data, self.events[5:10])
        self.assertEqual(len(buf.events), 8)

    def test_buffer_complete_overflow_limit(self):
        buf = _EventsBufferStrict(8)
        buf.append_event(self.events[0])
        buf.append_events(self.events[1:6])
        buf.append_event(self.events[6])
        buf.append_events(self.events[7:10])
        data, complete = buf.newer_than('xxxx-xxxx-xx02')
        self.assertTrue(complete)
        self.assertEqual(data, self.events[3:10])
        self.assertEqual(len(buf.events), 8)

    def test_buffer_incomplete_overflow_limit(self):
        buf = _EventsBufferStrict(8)
        buf.append_event(self.events[0])
        buf.append_events(self.events[1:6])
        buf.append_event(self.events[6])
        buf.append_events(self.events[7:10])
        data, complete = buf.newer_than('xxxx-xxxx-xx01')
        self.assertFalse(complete)
        self.assertEqual(data, self.events[2:10])
        self.assertEqual(len(buf.events), 8)

    def test_buffer_full_overflow(self):
        buf = _EventsBufferStrict(8)
        buf.append_events(self.events[0:6])
        buf.append_events(self.events[6:32])
        data, complete = buf.newer_than('xxxx-xxxx-xx29')
        self.assertTrue(complete)
        self.assertEqual(data, self.events[30:32])
        self.assertEqual(len(buf.events), 8)

    def test_buffer_many_removals(self):
        buf = _EventsBufferStrict(8)
        buf.append_events(self.events[0:6])
        buf.append_events(self.events[6:12])
        buf.append_events(self.events[12:17])
        buf.append_events(self.events[17:22])
        buf.append_events(self.events[22:28])
        buf.append_events(self.events[28:29])
        buf.append_events(self.events[29:36])
        buf.append_event(self.events[36])
        buf.append_events(self.events[37:42])
        buf.append_events(self.events[42:49])
        buf.append_events(self.events[49:54])
        data, complete = buf.newer_than('xxxx-xxxx-xx47')
        self.assertTrue(complete)
        self.assertEqual(data, self.events[48:54])
        self.assertEqual(len(buf.events), 8)

    def test_buffer_most_recent(self):
        buf = _EventsBufferStrict(8)
        buf.append_events(self.events[0:6])
        self.assertEqual(buf.most_recent(3), self.events[3:6])
        buf.append_events(self.events[6:10])
        self.assertEqual(buf.most_recent(8), self.events[2:10])
        buf.append_events(self.events[10:16])
        self.assertEqual(buf.most_recent(8), self.events[8:16])
        self.assertEqual(buf.most_recent(9), self.events[8:16])


class TestPersistentEventBuffer(unittest.TestCase):

    def setUp(self):
        self.events = [events.Event('test-application',
                                    'text/plain',
                                    'body {:02d}'.format(i),
                                    event_id='xxxx-xxxx-xx{:02d}'.format(i)) \
                       for i in range(100)]

    def test_buffer_complete_no_overflow(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            buf.append_event(self.events[0])
            buf.append_events(self.events[1:6])
            buf.append_event(self.events[6])
            data, complete = buf.newer_than('xxxx-xxxx-xx03')
            self.assertTrue(complete)
            self.assertEqual(data, self.events[4:7])
            self.assertEqual(len(buf.events), 7)
        finally:
            shutil.rmtree(base_dir)

    def test_buffer_complete_no_overflow_interrupted(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            buf.append_event(self.events[0])
            buf.append_events(self.events[1:6])
            # Interruption! Reload from disk:
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            buf.append_event(self.events[6])
            data, complete = buf.newer_than('xxxx-xxxx-xx03')
            self.assertTrue(complete)
            self.assertEqual(data, self.events[4:7])
            self.assertEqual(len(buf.events), 7)
        finally:
            shutil.rmtree(base_dir)

    def test_buffer_complete_overflow_interrupted(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            buf.append_event(self.events[0])
            buf.append_events(self.events[1:6])
            buf.append_event(self.events[6])
            buf.append_events(self.events[7:10])
            # Interruption! Reload from disk:
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            data, complete = buf.newer_than('xxxx-xxxx-xx04')
            self.assertTrue(complete)
            self.assertEqual(data, self.events[5:10])
            self.assertEqual(len(buf.events), 8)
        finally:
            shutil.rmtree(base_dir)

    def test_buffer_complete_overflow_limit_interrupted(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            buf.append_event(self.events[0])
            buf.append_events(self.events[1:6])
            buf.append_event(self.events[6])
            buf.append_events(self.events[7:10])
            # Interruption! Reload from disk:
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            data, complete = buf.newer_than('xxxx-xxxx-xx02')
            self.assertTrue(complete)
            self.assertEqual(data, self.events[3:10])
            self.assertEqual(len(buf.events), 8)
        finally:
            shutil.rmtree(base_dir)

    def test_buffer_incomplete_overflow_limit_interrupted(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            buf.append_event(self.events[0])
            buf.append_events(self.events[1:6])
            buf.append_event(self.events[6])
            buf.append_events(self.events[7:10])
            # Interruption! Reload from disk:
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            data, complete = buf.newer_than('xxxx-xxxx-xx01')
            self.assertFalse(complete)
            self.assertEqual(data, self.events[2:10])
            self.assertEqual(len(buf.events), 8)
        finally:
            shutil.rmtree(base_dir)

    def test_buffer_full_overflow_interrupted(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            buf.append_events(self.events[0:6])
            buf.append_events(self.events[6:32])
            # Interruption! Reload from disk:
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            data, complete = buf.newer_than('xxxx-xxxx-xx29')
            self.assertTrue(complete)
            self.assertEqual(data, self.events[30:32])
            self.assertEqual(len(buf.events), 8)
        finally:
            shutil.rmtree(base_dir)

    def test_buffer_many_removals_interrupted(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            buf.append_events(self.events[0:6])
            buf.append_events(self.events[6:12])
            buf.append_events(self.events[12:17])
            buf.append_events(self.events[17:22])
            buf.append_events(self.events[22:28])
            buf.append_events(self.events[28:29])
            buf.append_events(self.events[29:36])
            buf.append_event(self.events[36])
            buf.append_events(self.events[37:42])
            buf.append_events(self.events[42:49])
            # Interruption! Reload from disk:
            buf = _PersistentEventsBufferStrict(8, 'test-label',
                                                base_dir=base_dir)
            buf.append_events(self.events[49:54])
            data, complete = buf.newer_than('xxxx-xxxx-xx47')
            self.assertTrue(complete)
            self.assertEqual(data, self.events[48:54])
            self.assertEqual(len(buf.events), 8)
        finally:
            shutil.rmtree(base_dir)

    def test_buffer_wrong_size(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            _PersistentEventsBufferStrict(8, 'test-label',
                                          base_dir=base_dir)
            # Interruption! Reload from disk:
            with self.assertRaises(ValueError):
                _PersistentEventsBufferStrict(10, 'test-label',
                                              base_dir=base_dir)
        finally:
            shutil.rmtree(base_dir)


class _MockEvent(object):
    def __init__(self, event_id):
        self.event_id = event_id

    def __str__(self):
        return self.event_id


class _EventsBufferStrict(EventsBuffer):
    """Force a key error when removing an element not in the dict."""
    def _remove_from_dict(self, position, num_events):
        for i in range(position, position + num_events):
            if self.buffer[i] is not None:
                del self.events[self.buffer[i].event_id]


class _PersistentEventsBufferStrict(PersistentEventsBuffer):
    """Force a key error when removing an element not in the dict."""
    def _remove_from_dict(self, position, num_events):
        for i in range(position, position + num_events):
            if self.buffer[i] is not None:
                del self.events[self.buffer[i].event_id]
