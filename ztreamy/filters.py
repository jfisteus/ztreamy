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
# <http://www.gnu.org//licenses/>.
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
import inspect

import rdflib
from rdflib.plugins.sparql import prepareQuery
from pyparsing import Word, Literal, NotAny, QuotedString, Group, Forward, \
                      Keyword, ZeroOrMore, printables, alphanums

from ztreamy import ZtreamyException, RDFEvent


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


class EventTypeFilter(Filter):
    def __init__(self, callback, event_types, application_ids=[]):
        """Creates a filter for application ids.

        'application_ids' must be a list of ids. If empty,
        any application_id is accepted.

        'event_types' must be a list of event types. It cannot be empty.

        """
        if event_types is None or not len(event_types):
            raise ValueError('Empty Event-Type filter')
        super(EventTypeFilter, self).__init__(callback)
        if application_ids is not None:
            self.application_ids = set(application_ids)
        self.event_types = set(event_types)
        if len(self.application_ids):
            self.filter_event = self._filter_event_with_application_id
        else:
            self.filter_event = self._filter_event_without_application_id

    def _filter_event_with_application_id(self, event):
        if (event.application_id in self.application_ids
            and event.event_type in self.event_types):
            self.callback(event)

    def _filter_event_without_application_id(self, event):
        if event.event_type in self.event_types:
            self.callback(event)


class VocabularyFilter(Filter):
    def __init__(self, callback, uri_prefixes):
        """Creates a filter based on prefixes of the URIs of the event.

        'uri_prefixes' has to be a string or a list of strings. The
        events that contain a URI matching at least one of the
        prefixes are selected. The rest are filtered out.

        """
        super(VocabularyFilter, self).__init__(callback)
        if isinstance(uri_prefixes, basestring):
            self.uri_prefixes = [uri_prefixes]
        else:
            self.uri_prefixes = uri_prefixes

    def filter_event(self, event):
        if self.callback is not None and isinstance(event, RDFEvent):
            for triple in event.body:
                if (self._test_uri(triple[0])
                    or self._test_uri(triple[1])
                    or self._test_uri(triple[2])):
                    self.callback(event)
                    break

    def _test_uri(self, uriref):
        if isinstance(uriref, rdflib.term.URIRef):
            for prefix in self.uri_prefixes:
                if uriref.startswith(prefix):
                    return True
        return False


class SimpleTripleFilter(Filter):
    """Filters events matching a given pattern of subject, predicate and object

    Only events that contain at least one triple with the specified
    subject, predicate and object pass the filter. Not all the three
    terms of the triple need to be specified, in which case they act
    as wildcards.

    """
    def __init__(self, callback, subject=None, predicate=None, object_=None,
                 object_literal=None):
        """Creates a filter for RDF triples.

        `subject`, `predicate`, `object_` can be None. All the three
        must be strings representing a URI. Use None as a wildcard to
        match any triple, or just omit the corresponding keyword
        argument. `object_literal` is a string representing a literal
        value, and is incompatible with `object_`.

        """
        super(SimpleTripleFilter, self).__init__(callback)
        assert object_ is None or object_literal is None
        subject_p = rdflib.term.URIRef(subject) if subject else None
        predicate_p = rdflib.term.URIRef(predicate) if predicate else None
        if object_ is not None:
            object_p = rdflib.term.URIRef(object_)
        elif object_literal is not None:
            object_p = rdflib.term.Literal(object_literal)
        else:
            object_p = None
        self.pattern = (subject_p, predicate_p, object_p)

    def filter_event(self, event):
        if self.callback is not None and isinstance(event, RDFEvent):
            try:
                generator = event.body.triples(self.pattern)
                generator.next()
            except StopIteration:
                pass
            else:
                self.callback(event)


class SPARQLFilter(Filter):
    """Filters events matching a given SPARQL ASK query.

    Only the events that match the query can pass the filter.

    """
    def __init__(self, callback, sparql_query):
        """Creates a SPARQL filter for RDF triples."""
        if sparql_query.strip()[:3].lower() != 'ask':
            raise ZtreamyException('Only ASK queries are allowed '
                                   'in SPARQLFilter')
        super(SPARQLFilter, self).__init__(callback)
        self.query = prepareQuery(sparql_query)

    def filter_event(self, event):
        if self.callback is not None and isinstance(event, RDFEvent):
            if event.body.query(self.query).askAnswer:
                self.callback(event)


class TripleFilter(SPARQLFilter):
    """Filters events containing certain triple patterns.

    Only the events that match the filter pattern can pass the filter.
    These are some sample patterns:

    Example 1:
    <http://example.com/me http://example.com/liveAt http://example.com/here>

    Example 2 (with a wildcard):
    < * http://example.com/liveAt http://example.com/here>

    Example 3 (AND pattern):
    <http://example.com/me http://example.com/liveAt http://example.com/here>
    AND
    <http://example.com/me http://example.com/hasName "Paul">

    Example 4 (OR pattern and parenthesis):
    <http://example.com/me http://example.com/liveAt http://example.com/here>
    AND
    ( <http://example.com/me http://example.com/hasName "Paul">
      OR
      <http://example.com/me http://example.com/hasName "Maria">
    )

    Example 5 (variables):
    <http://example.com/me http://example.com/liveAt ?place>
    AND
    ?place http://example.com/locatedIn http://example.com/instances/France>

    """
    def __init__(self, callback, filter_expression):
        """Creates a filter for RDF triples."""
        sparql_query = _triple_filter_to_sparql(filter_expression)
        ## print(sparql_query)
        super(TripleFilter, self).__init__(callback, sparql_query)


class SynchronousFilter(object):
    """ Converts a normal (asynchronous) filter into synchronous.

    Example:

    filter_ = filters.SynchronousFilter(filters.EventTypeFilter,
                                        ['TypeA', 'TypeB'],
                                        application_ids=['AppA'])
     if filter_.filter_event(event):
        # do something
    (...)
    accepted_events = filter_.filter_events(events)

    """
    def __init__(self, async_filter_class, *filter_args, **filter_kwargs):
        if not inspect.isclass(async_filter_class):
            raise ValueError('The SynchronousFilter constructor '
                             'expects a class')
        # Create an instance of the filter class
        self.async_filter = async_filter_class(self._callback, *filter_args,
                                               **filter_kwargs)
        self.events = []

    def filter_event(self, event):
        """ Return True if the event passes the filter, False otherwise."""
        self.async_filter.filter_event(event)
        if self.events:
            result = True
        else:
            result = False
        self.events = []
        return result

    def filter_events(self, events):
        """ Return the list of events that pass the filter."""
        self.async_filter.filter_events(events)
        result = self.events
        self.events = []
        return result

    def _callback(self, event):
        self.events.append(event)


#
# Parser for filtering expressions.
#
_filter_parser = None
def _create_filter_parser():
    and_kw = Keyword('AND')
    or_kw = Keyword('OR')
    variable = Literal('?') + Word(alphanums + '_').leaveWhitespace()
    uri_term = NotAny(Literal('"')) + Word(printables, excludeChars='>*')
    uri_part = Keyword('*') ^ uri_term ^ variable
    literal_term = QuotedString(quoteChar='"', escChar='\\')
    triple = Group(Literal('<').suppress() + uri_part.setResultsName('subj')
                   + uri_part.setResultsName('pred')
                   + (Group(uri_part).setResultsName('obj')
                      ^ Group(literal_term).setResultsName('objlit'))
                   + Literal('>').suppress())
    expr = Forward()
    atom = (triple.setResultsName('triple')
            | Literal('(').suppress() + expr + Literal(')').suppress())
    and_group = Group(atom + ZeroOrMore(and_kw.suppress() + atom))
    or_group = Group(atom + ZeroOrMore(or_kw.suppress() + atom))
    expr << (and_group.setResultsName('and') ^ or_group.setResultsName('or'))
    return expr

def _triple_filter_to_sparql(text):
    global _filter_parser
    if _filter_parser is None:
        _filter_parser = _create_filter_parser()
    return _build_sparql(_filter_parser.parseString(text, parseAll=True))

def _build_sparql(parse_results):
    parts = ['ASK']
    pattern = _build_sparql_internal(parse_results, [0])
    parts.append('{')
    parts.extend(pattern)
    parts.append('}')
    return ' '.join(parts)

def _build_sparql_internal(parse_results, num_used_variables):
    parts = []
    if parse_results.getName() == 'triple':
        parts.append(_represent_uri_term(parse_results[0],
                                         num_used_variables))
        parts.append(_represent_uri_term(parse_results[1],
                                         num_used_variables))
        if parse_results[2].getName() == 'objlit':
            parts.append('"' + parse_results[2][0] + '"')
        else:
            parts.append(_represent_uri_term(parse_results[2][0],
                                             num_used_variables))
        parts.append('.')
    elif parse_results.getName() == 'and':
        if len(parse_results) < 2:
            parts = _build_sparql_internal(parse_results[0],
                                           num_used_variables)
        else:
            for expr in parse_results:
                parts.extend(_build_sparql_internal(expr, num_used_variables))
    elif parse_results.getName() == 'or':
        if len(parse_results) < 2:
            parts = _build_sparql_internal(parse_results[0],
                                           num_used_variables)
        else:
            ## parts.append('{')
            operands = []
            for expr in parse_results:
                operands.append(' '.join(_build_sparql_internal(expr,
                                                          num_used_variables)))
            parts.append('{ ' + ' } UNION { '.join(operands) + ' }')
            ## parts.append('}')
    return parts

def _represent_uri_term(parse_results, num_used_variables):
    if parse_results == '*':
        result = '?var_int__' + str(num_used_variables[0])
        num_used_variables[0] += 1
    elif parse_results.startswith('?'):
        result = parse_results
    else:
        result = '<' + parse_results + '>'
    return result
