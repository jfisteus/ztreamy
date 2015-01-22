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
