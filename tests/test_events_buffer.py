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

from ztreamy.events_buffer import (EventsBuffer,
                                   PersistentEventsBuffer,
                                   PendingEventsBuffer,
                                   PersistentPendingEventsBuffer)
from ztreamy import events


class TestEventBuffer(unittest.TestCase):

    def setUp(self):
        self.events = _create_mock_events(100)

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
        self.events = _create_mock_events(100)

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


class TestPendingEventsBuffer(unittest.TestCase):

    def setUp(self):
        self.events = _create_mock_events(20)

    def test_simple(self):
        buf = PendingEventsBuffer()
        self.assertEqual(buf.get_events(), [])
        self.assertTrue(buf.add_event(self.events[0]))
        self.assertTrue(buf.add_event(self.events[1]))
        self.assertTrue(buf.add_event(self.events[2]))
        self.assertEqual(buf.get_events(), self.events[:3])
        self.assertEqual(buf.get_events(), [])

    def test_duplicate(self):
        buf = PendingEventsBuffer()
        self.assertTrue(buf.add_event(self.events[0]))
        self.assertTrue(buf.add_event(self.events[1]))
        self.assertTrue(buf.add_event(self.events[2]))
        duplicate = _MockEvent(self.events[1].event_id)
        self.assertFalse(buf.add_event(duplicate))
        self.assertTrue(buf.add_event(self.events[3]))
        self.assertEqual(buf.get_events(), self.events[:4])
        self.assertEqual(buf.get_events(), [])
        self.assertTrue(buf.add_event(self.events[2]))
        self.assertEqual(buf.get_events(),  [self.events[2]])

    def test_is_duplicate(self):
        buf = PendingEventsBuffer()
        self.assertTrue(buf.add_event(self.events[0]))
        self.assertTrue(buf.add_event(self.events[1]))
        self.assertTrue(buf.add_event(self.events[2]))
        duplicate = _MockEvent(self.events[1].event_id)
        self.assertTrue(buf.is_duplicate(duplicate))

    def test_add_events(self):
        buf = PendingEventsBuffer()
        self.assertTrue(buf.add_event(self.events[0]))
        accepted = buf.add_events(self.events[1:7])
        self.assertEqual(accepted, self.events[1:7])
        duplicate = _MockEvent(self.events[5].event_id)
        to_insert = self.events[7:10] + [duplicate] + self.events[10:13]
        accepted = buf.add_events(to_insert)
        self.assertEqual(accepted, self.events[7:13])
        self.assertEqual(buf.get_events(), self.events[:13])


class TestPersistentPendingEventsBuffer(unittest.TestCase):

    def setUp(self):
        self.events = _create_mock_events(20)

    def test_simple(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            buf = PersistentPendingEventsBuffer('test-label',
                                                base_dir=base_dir)
            self.assertEqual(buf.get_events(), [])
            self.assertTrue(buf.add_event(self.events[0]))
            self.assertTrue(buf.add_event(self.events[1]))
            # Interruption! Reload from disk:
            buf = PersistentPendingEventsBuffer('test-label',
                                                base_dir=base_dir)
            self.assertTrue(buf.add_event(self.events[2]))
            self.assertEqual(buf.get_events(), self.events[:3])
            # Interruption! Reload from disk:
            buf = PersistentPendingEventsBuffer('test-label',
                                                base_dir=base_dir)
            self.assertEqual(buf.get_events(), [])
        finally:
            shutil.rmtree(base_dir)

    def test_duplicate(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            buf = PersistentPendingEventsBuffer('test-label',
                                                base_dir=base_dir)
            self.assertTrue(buf.add_event(self.events[0]))
            self.assertTrue(buf.add_event(self.events[1]))
            self.assertTrue(buf.add_event(self.events[2]))
            # Interruption! Reload from disk:
            buf = PersistentPendingEventsBuffer('test-label',
                                                base_dir=base_dir)
            duplicate = _MockEvent(self.events[1].event_id)
            self.assertFalse(buf.add_event(duplicate))
            self.assertTrue(buf.add_event(self.events[3]))
            self.assertEqual(buf.get_events(), self.events[:4])
            self.assertEqual(buf.get_events(), [])
            self.assertTrue(buf.add_event(self.events[2]))
            # Interruption! Reload from disk:
            buf = PersistentPendingEventsBuffer('test-label',
                                                base_dir=base_dir)
            self.assertEqual(buf.get_events(),  [self.events[2]])
        finally:
            shutil.rmtree(base_dir)

    def test_is_duplicate(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            buf = PersistentPendingEventsBuffer('test-label',
                                                base_dir=base_dir)
            self.assertTrue(buf.add_event(self.events[0]))
            self.assertTrue(buf.add_event(self.events[1]))
            self.assertTrue(buf.add_event(self.events[2]))
            duplicate = _MockEvent(self.events[1].event_id)
            # Interruption! Reload from disk:
            buf = PersistentPendingEventsBuffer('test-label',
                                                base_dir=base_dir)
            self.assertTrue(buf.is_duplicate(duplicate))
        finally:
            shutil.rmtree(base_dir)

    def test_add_events(self):
        base_dir = None
        try:
            base_dir = tempfile.mkdtemp()
            buf = PersistentPendingEventsBuffer('test-label',
                                                base_dir=base_dir)
            self.assertTrue(buf.add_event(self.events[0]))
            accepted = buf.add_events(self.events[1:7])
            self.assertEqual(accepted, self.events[1:7])
            # Interruption! Reload from disk:
            buf = PersistentPendingEventsBuffer('test-label',
                                                base_dir=base_dir)
            duplicate = _MockEvent(self.events[5].event_id)
            to_insert = self.events[7:10] + [duplicate] + self.events[10:13]
            accepted = buf.add_events(to_insert)
            self.assertEqual(accepted, self.events[7:13])
            self.assertEqual(buf.get_events(), self.events[:13])
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


def _create_mock_events(num):
    return [events.Event('test-application',
                         'text/plain',
                         'body {:02d}'.format(i),
                         event_id='xxxx-xxxx-xx{:02d}'.format(i)) \
            for i in range(100)]
