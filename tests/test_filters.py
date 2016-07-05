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

import ztreamy.events as events
import ztreamy.filters as filters
import ztreamy.rdfevents as rdfevents


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


class TestTripleFilter(unittest.TestCase):
    test_graphs = [
        """@prefix e: <http://example.com/> .
           e:me e:liveAt e:here .
           e:you e:liveAt e:there .
           e:me e:state e:happy .
        """,
        """@prefix e: <http://example.com/> .
           e:you e:liveAt e:there .
           e:me e:state e:happy .
        """,
        """@prefix e: <http://example.com/> .
           e:me e:state e:happy .
        """,
        """@prefix e: <http://example.com/> .
           e:you e:state e:happy .
        """,
    ]
    test_events = [rdfevents.RDFEvent('', 'text/n3', g) for g in test_graphs]

    def test_triple_filter_one_triple(self):
        expr = ('<http://example.com/me '
                'http://example.com/liveAt '
                'http://example.com/here>')
        test_events = TestTripleFilter.test_events
        expected_events = [test_events[0]]
        self._check_filter(expr, test_events, expected_events)

    def test_triple_filter_or(self):
        expr = ('<http://example.com/me '
                'http://example.com/liveAt '
                'http://example.com/here> '
                'OR '
                '<http://example.com/me '
                'http://example.com/state '
                'http://example.com/happy>'
                )
        test_events = TestTripleFilter.test_events
        expected_events = test_events[:3]
        self._check_filter(expr, test_events, expected_events)

    def test_triple_filter_and(self):
        expr = ('<http://example.com/me '
                'http://example.com/liveAt '
                'http://example.com/here> '
                'AND '
                '<http://example.com/me '
                'http://example.com/state '
                'http://example.com/happy>'
                )
        test_events = TestTripleFilter.test_events
        expected_events = [test_events[0]]
        self._check_filter(expr, test_events, expected_events)

    def test_triple_filter_asterisk(self):
        expr = ('<* '
                'http://example.com/liveAt '
                'http://example.com/there> '
                )
        test_events = TestTripleFilter.test_events
        expected_events = test_events[:2]
        self._check_filter(expr, test_events, expected_events)

    def test_triple_filter_asterisk_and(self):
        expr = ('<* '
                'http://example.com/liveAt '
                'http://example.com/there> '
                'AND '
                '<* '
                'http://example.com/liveAt '
                'http://example.com/here> '
                )
        test_events = TestTripleFilter.test_events
        expected_events = [test_events[0]]
        self._check_filter(expr, test_events, expected_events)

    def test_triple_filter_asterisk_multiple(self):
        expr = ('<http://example.com/me '
                '* '
                '*>'
                )
        test_events = TestTripleFilter.test_events
        expected_events = test_events[:3]
        self._check_filter(expr, test_events, expected_events)

    def test_triple_filter_parenthesis_1(self):
        expr = ('(<http://example.com/you '
                'http://example.com/liveAt '
                'http://example.com/there> '
                'AND '
                '<http://example.com/me '
                'http://example.com/liveAt '
                'http://example.com/here> '
                ') OR '
                '<http://example.com/me '
                'http://example.com/state '
                'http://example.com/happy>'
                )
        test_events = TestTripleFilter.test_events
        expected_events = test_events[:3]
        self._check_filter(expr, test_events, expected_events)

    def test_triple_filter_parenthesis_2(self):
        expr = ('<http://example.com/me '
                'http://example.com/liveAt '
                'http://example.com/here> '
                'AND '
                '(<http://example.com/you '
                'http://example.com/liveAt '
                'http://example.com/there> '
                ' OR '
                '<http://example.com/me '
                'http://example.com/state '
                'http://example.com/happy>)'
                )
        test_events = TestTripleFilter.test_events
        expected_events = test_events[:1]
        self._check_filter(expr, test_events, expected_events)

    def _check_filter(self, expr, test_events, expected_events):
        callback = _FilterCallback()
        filter_ = filters.TripleFilter(callback.callback, expr)
        for event in test_events:
            filter_.filter_event(event)
        self.assertEqual(expected_events, callback.events)


class TestSynchronousFilter(unittest.TestCase):

    def test_synchronous_filter(self):
        filter_ = filters.SynchronousFilter(filters.Filter)
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
        self.assertFalse(filter_.filter_event(test_events[0]))
        self.assertEqual([], filter_.filter_events(test_events))

    def test_synchronous_filter_args(self):
        filter_ = filters.SynchronousFilter(filters.VocabularyFilter,
                                            ['http://example.com/'])
        test_graphs = [
            """@prefix e: <http://ample.com/> .
               e:me e:liveAt e:here .
               e:you e:liveAt e:there .
               e:me e:state e:happy .
            """,
            """@prefix e: <http://example.com/> .
               e:me e:liveAt e:here .
               e:you e:liveAt e:there .
               e:me e:state e:happy .
            """,
        ]
        test_events = [rdfevents.RDFEvent('', 'text/n3', g) \
                       for g in test_graphs]
        self.assertEqual([test_events[1]], filter_.filter_events(test_events))
        self.assertFalse(filter_.filter_event(test_events[0]))
        self.assertTrue(filter_.filter_event(test_events[1]))
        self.assertFalse(filter_.filter_event(test_events[0]))

    def test_synchronous_filter_kwargs(self):
        filter_ = filters.SynchronousFilter(filters.EventTypeFilter,
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
        # Check first this one to test that no memory is kept in the filter
        self.assertTrue(filter_.filter_event(test_events[2]))
        self.assertFalse(filter_.filter_event(test_events[0]))
        self.assertFalse(filter_.filter_event(test_events[1]))
        self.assertEqual([test_events[2], test_events[4]],
                         filter_.filter_events(test_events))
        self.assertEqual([], filter_.filter_events(test_events[:2]))


class _FilterCallback(object):
    def __init__(self):
        self.events = []

    def callback(self, event):
        self.events.append(event)
