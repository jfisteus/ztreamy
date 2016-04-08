# -*- coding: utf-8 -*-
# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2015 Jesus Arias Fisteus
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
import itertools

import ztreamy
import ztreamy.events as events

class TestEvent(unittest.TestCase):

    def test_error_property_type(self):
        source_id = 'source-id-value'
        syntax = ztreamy.json_media_type
        body = {'a': 1, 'b': 'blah!'}
        e = events.Event(source_id, syntax, body)
        events.Event(source_id, syntax, body, event_id='6767676766')
        events.Event(source_id, syntax, body,
                     aggregator_id=['6767676766', '788839393'])
        with self.assertRaises(ValueError):
            events.Event(source_id, syntax, body, event_id={'a': 5})
        with self.assertRaises(ValueError):
            events.Event({}, syntax, body)
        with self.assertRaises(ValueError):
            e.aggregator_id = ['2334343434', {'a': 5}]
        e.aggregator_id = ['2334343434', '435345345345']

    def test_error_extra_header(self):
        source_id = 'source-id-value'
        syntax = ztreamy.json_media_type
        body = {'a': 1, 'b': 'blah!'}
        extra = {'x-a': '23', 'x-b': 'df4'}
        e = events.Event(source_id, syntax, body, extra_headers=extra)
        e.set_extra_header('x-c', '565655656')
        with self.assertRaises(ValueError):
            e.set_extra_header('x-c', 565655656)
        with self.assertRaises(ValueError):
            e.set_extra_header('Body', 'sdfsdfsdf')
        with self.assertRaises(ValueError):
            e.set_extra_header('Source-Id', 'sdfsdfsdf')

    def test_serialize_ldjson(self):
        source_id = 'source-id-value'
        syntax = ztreamy.json_media_type
        body = {'a': 1, 'b': 'blah!'}
        evs = [
            events.JSONEvent(source_id, syntax, body),
            events.JSONEvent(source_id, syntax, body, event_id='6767676766'),
            events.JSONEvent(source_id, syntax, body,
                             aggregator_id=['6767676766', '788839393']),
        ]
        serialized = ztreamy.serialize_events_ldjson(evs)
        parts = serialized.split('\r\n')
        self.assertEqual(len(parts), 4)
        self.assertEqual(parts[3], '')
        deserializer = ztreamy.JSONDeserializer()
        evs2 = list(itertools.chain( \
                    *[deserializer.deserialize(part) for part in parts[:-1]]))
        self.assertEqual(evs[0].source_id, evs2[0].source_id)
        self.assertEqual(evs[0].syntax, evs2[0].syntax)
        self.assertEqual(evs[0].body, evs2[0].body)
        self.assertEqual(evs[1].source_id, evs2[1].source_id)
        self.assertEqual(evs[1].syntax, evs2[1].syntax)
        self.assertEqual(evs[1].body, evs2[1].body)
        self.assertEqual(evs[1].event_id, evs2[1].event_id)
        self.assertEqual(evs[2].source_id, evs2[2].source_id)
        self.assertEqual(evs[2].syntax, evs2[2].syntax)
        self.assertEqual(evs[2].body, evs2[2].body)
        self.assertEqual(evs[2].aggregator_id, evs2[2].aggregator_id)

    def test_unicode_fields(self):
        event = ztreamy.Event(u'7272727828',
                              u'text/plain',
                              u'ásddf'.encode('utf-8'),
                              event_id=u'777888333',
                              application_id=u'app',
                              aggregator_id=[u'234234234', '3333', u'324234'],
                              event_type=u'TestType',
                              timestamp=u'20150714T16:29:00+0200',
                              extra_headers={u'X-Header': u'value'})
        serialized = str(event)
        deserializer = ztreamy.Deserializer()
        deserialized = deserializer.deserialize(serialized, complete=True)
        self.assertEqual(len(deserialized), 1)
        self.assertEqual(event.source_id, deserialized[0].source_id)
        self.assertEqual(event.syntax, deserialized[0].syntax)
        self.assertEqual(event.body, deserialized[0].body)
        self.assertEqual(event.event_id, deserialized[0].event_id)
        self.assertEqual(event.aggregator_id, deserialized[0].aggregator_id)
        self.assertEqual(event.event_type, deserialized[0].event_type)
        self.assertEqual(event.timestamp, deserialized[0].timestamp)
        self.assertEqual(event.extra_headers, deserialized[0].extra_headers)

    def test_unicode_fields_json(self):
        event = ztreamy.Event(u'7272727828',
                              u'text/plain',
                              u'ásddf'.encode('utf-8'),
                              event_id=u'777888333',
                              application_id=u'app',
                              aggregator_id=[u'234234234', '3333', u'324234'],
                              event_type=u'TestType',
                              timestamp=u'20150714T16:29:00+0200',
                              extra_headers={u'X-Header': u'value'})
        serialized = event.serialize_json()
        deserializer = ztreamy.JSONDeserializer()
        deserialized = deserializer.deserialize(serialized, complete=True)
        self.assertEqual(len(deserialized), 1)
        self.assertEqual(event.source_id, deserialized[0].source_id)
        self.assertEqual(event.syntax, deserialized[0].syntax)
        self.assertEqual(event.body, deserialized[0].body)
        self.assertEqual(event.event_id, deserialized[0].event_id)
        self.assertEqual(event.aggregator_id, deserialized[0].aggregator_id)
        self.assertEqual(event.event_type, deserialized[0].event_type)
        self.assertEqual(event.timestamp, deserialized[0].timestamp)
        self.assertEqual(event.extra_headers, deserialized[0].extra_headers)

    def test_timestamp(self):
        t = '1970-01-01T10:45:02Z'
        event = ztreamy.Event('7272727828',
                              'text/plain',
                              'asddf',
                              timestamp=t)
        self.assertEqual(event.timestamp, t)
        self.assertIsNone(event._time)
        self.assertEqual(event.time, 38702.0)
        self.assertIsNotNone(event._time)
        # Check that .time does not cache the old value:
        t = '2016-04-06T11:15:02+00:00'
        event.timestamp = t
        self.assertEqual(event.timestamp, t)
        self.assertIsNone(event._time)
        self.assertEqual(event.time, 1459941302.0)
        self.assertIsNotNone(event._time)

    def test_timestamp_errors(self):
        # Timestamps with no timezone are not allowed:
        event = ztreamy.Event('7272727828',
                              'text/plain',
                              'asddf',
                              timestamp='1970-01-01T10:45:02')
        # Note that lazy timestamp parsing is used
        self.assertEqual(event.timestamp, '1970-01-01T10:45:02')
        with self.assertRaises(ztreamy.ZtreamyException):
            event.time

        # None timestamps are not allowed:
        with self.assertRaises(ValueError):
            event.timestamp = None
