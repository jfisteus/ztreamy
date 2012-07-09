# streamsem: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2012 Jesus Arias Fisteus
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
# <http://www.gnu.org/
#
"""General-purpose event filters.

This module defines filters that select events according to specific
criteria.

Filters are expected to have:

(1) A constructor that receives a callback function. The callback
function is called for those events that are not blocked by the
filter.

(2) A 'filter_event()' method that receives an event and, it the event
is not filtered out, invokes the callback function with it.

(3) A 'filter_events()' method, with a similar behaviour, that
receives a list or iterable of events instead of a single event.

A way of implementing a custom filter is to extend the 'Filter' class
and override its 'filter_event()' method.

"""
import rdflib

from streamsem import rdfevents

class Filter(object):
    """ A default filter that filters out every event.

    This class can be used for extending other filters from it.

    """
    def __init__(self, callback):
        """Creates a new filter.

        `callback` is the function to call for every event matching
        the filter.

        """
        self.callback = callback

    def filter_event(self, event):
        """The filter executes the callback for every matching event.

        This implementation just filters out all the events.

        """
        pass

    def filter_events(self, events):
        """Convenience method when an iterable of events is available.

        """
        for event in events:
            self.filter_event(event)


class SourceFilter(Filter):
    def __init__(self, callback, source_id=None, source_ids=[]):
        """Creates a filter for source ids.

        'source_id' must be only one id, whereas 'source_ids' must be
        a list of ids. If both are present, 'source_id' is appended to
        the list of ids.

        """
        super(SourceFilter, self).__init__(callback)
        self.source_ids = set()
        if source_id is not None:
            self.source_ids.add(source_id)
        for source in source_ids:
            self.source_ids.add(source)

    def filter_event(self, event):
        if event.source_id in self.source_ids:
            self.callback(event)


class ApplicationFilter(Filter):
    def __init__(self, callback, application_id=None, application_ids=[]):
        """Creates a filter for application ids.

        'application_id' must be only one id, whereas
        'application_ids' must be a list of ids. If both are present,
        'application_id' is appended to the list of ids.

        """
        super(ApplicationFilter, self).__init__(callback)
        self.application_ids = set()
        if application_id is not None:
            self.application_ids.add(application_id)
        for application in application_ids:
            self.application_ids.add(application)

    def filter_event(self, event):
        if event.application_id in self.application_ids:
            self.callback(event)


class SimpleTripleFilter(Filter):
    def __init__(self, callback, subject=None, predicate=None, object_=None,
                 object_literal=None):
        """Creates a filter for RDF triples.

        `subject`, `predicate`, `object_` can be None. All the three
        must be strings representing a URI. Use None as a wildcard to
        match any triple. `object_literal` is a string representing a
        literal value, and is incompatible with `object_`.

        """
        assert object_ is None or object_literal is None
        self.callback = callback
        self.subject = rdflib.term.URIRef(subject) if subject else None
        self.predicate = rdflib.term.URIRef(predicate) if predicate else None
        self.object = rdflib.term.URIRef(object_) \
            if object_ else object_literal

    def filter_event(self, event):
        if self.callback is not None and isinstance(event, rdfevents.RDFEvent):
            if self._matches(event.body):
                self.callback(event)

    def _matches(self, graph):
        gen = graph.triples((self.subject, self.predicate, self.object))
        try:
            gen.next()
            matches = True
        except StopIteration:
            matches = False
        gen.close()
        return matches
