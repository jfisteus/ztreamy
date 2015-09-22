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

import ztreamy.events as events
import ztreamy.filters as filters

class TestEventTypeFilter(unittest.TestCase):

    def test_with_application_id(self):
        callback = _FilterCallback()
        filter_ = filters.EventTypeFilter(callback.callback,
                                          ['TypeA', 'TypeB'],
                                          application_ids=['AppA'])
        test_events = [
            events.Event('', 'text/plain', '',
                         event_type='TypeC', application_id='AppB'),
            events.Event('', 'text/plain', '',
                         event_type='TypeA'),
            events.Event('', 'text/plain', '',
                         event_type='TypeA', application_id='AppA'),
            events.Event('', 'text/plain', '',
                         event_type='TypeC', application_id='AppA'),
            events.Event('', 'text/plain', '',
                         event_type='TypeB', application_id='AppA'),
            events.Event('', 'text/plain', '',
                         event_type='TypeA', application_id='AppB'),
            events.Event('', 'text/plain', '',
                         event_type='TypeB', application_id='AppB'),
        ]
        for event in test_events:
            filter_.filter_event(event)
        self.assertEqual(len(callback.events), 2)
        self.assertEqual(id(callback.events[0]), id(test_events[2]))
        self.assertEqual(id(callback.events[1]), id(test_events[4]))


    def test_without_application_id(self):
        callback = _FilterCallback()
        filter_ = filters.EventTypeFilter(callback.callback,
                                          ['TypeA', 'TypeB'])
        test_events = [
            events.Event('', 'text/plain', '',
                         event_type='TypeC', application_id='AppB'),
            events.Event('', 'text/plain', '',
                         event_type='TypeA'),
            events.Event('', 'text/plain', '',
                         event_type='TypeA', application_id='AppA'),
            events.Event('', 'text/plain', '',
                         event_type='TypeC', application_id='AppA'),
            events.Event('', 'text/plain', '',
                         event_type='TypeB', application_id='AppA'),
            events.Event('', 'text/plain', '',
                         event_type='TypeA', application_id='AppB'),
            events.Event('', 'text/plain', '',
                         event_type='TypeB', application_id='AppB'),
        ]
        for event in test_events:
            filter_.filter_event(event)
        self.assertEqual(len(callback.events), 5)
        self.assertEqual(id(callback.events[0]), id(test_events[1]))
        self.assertEqual(id(callback.events[1]), id(test_events[2]))
        self.assertEqual(id(callback.events[2]), id(test_events[4]))
        self.assertEqual(id(callback.events[3]), id(test_events[5]))
        self.assertEqual(id(callback.events[4]), id(test_events[6]))


class _FilterCallback(object):
    def __init__(self):
        self.events = []

    def callback(self, event):
        self.events.append(event)
