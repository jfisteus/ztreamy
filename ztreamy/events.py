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
"""Representation and  manipulation of events.

"""
import logging
import time
import json
import copy

import ztreamy
from ztreamy import ZtreamyException


class Deserializer(object):
    """Object that deserializes events.

    The deserializer processes the data from an internal data buffer
    in a stream model: data chunks can be continously added to the
    buffer and parsed. Data chunks do not need to finish at complete
    events. When a partial event is at the end of a chunk, its data is
    maintained for the next parse attempt.

    It maintains a context, so a separate deserialized must be used
    for each event client, in order to not mix the contexts of
    different events.

    A normal workflow is:

    deserializer = Deserializer()
    while new_data arrives:
        events = deserializer.deserialize(new_data)

    """
    def __init__(self):
        """Creates a new 'Deserializer' object."""
        self.reset()

    def append_data(self, data):
        """Appends new data to the data buffer of the deserializer."""
        self._data = self._data + data
        self._previous_len = len(self._data)

    def data_consumed(self):
        """Amount of bytes consumed since the last 'append_data()'."""
        return self._previous_len - len(self._data)

    def reset(self):
        """Resets the state of the parser and discards pending data."""
        self._data = ''
        self.previous_len = 0
        self._event_reset()
        self.warning_lf_eol_reported = False

    def data_is_pending(self):
        return self._data != ''

    def _event_reset(self):
        """Method to be called internally after an event is read."""
        self._event = {}
        self._extra_headers = {}
        self._header_complete = False

    def deserialize(self, data, parse_body=True, complete=False):
        """Deserializes and returns a list of events.

        Deserializes all the events until no more events can be parsed
        from the data stored in this deserializer object. The remaining
        data is kept for being parsed in future calls to this method.

        If 'data' is provided, it is appended to the data buffer. It
        may be None.

        If 'parse_body' is True (the default value), the parser will
        deserialize also the body of the events according to their
        types. If not, their body will be stored only in their
        serialized form (as a string).

        The list of deserialized event objects is returned. The list
        is empty when no events are deserialized.

        """
        if data is not None:
            self.append_data(data)
        events = []
        event = self.deserialize_next(parse_body=parse_body)
        while event is not None:
            events.append(event)
            event = self.deserialize_next(parse_body=parse_body)
        if complete and len(self._data) > 0:
            self.reset()
            raise ZtreamyException('Spurious data in the input event',
                                   'event_deserialize')
        return events

    def deserialize_next(self, parse_body=True):
        """Deserializes and returns an event from the data buffer.

        Returns None and keeps the pending data stored when a complete
        event is not in the stored data fragment.

        If 'parse_body' is True (the default value), the parser will
        deserialize also the body of the events according to their
        types. If not, their body will be stored only in their
        serialized form (as a string).

        """
        # Read headers
        pos = 0
        while not self._header_complete and pos < len(self._data):
            end = self._data.find('\n', pos)
            if end == -1:
                self._data = self._data[pos:]
                return None
            part = self._data[pos:end]
            # End-of-line delimiter is CRLF; LF is accepted but deprecated
            if (not self.warning_lf_eol_reported
                and (not part or part[-1] != '\r')):
                self.warning_lf_eol_reported = True
                logging.warning('LF end-of-line received, but CRLF expected. '
                                'LF EOLs are deprecated.')
            pos = end + 1
            if not part or part == '\r':
                self._header_complete = True
                break
            comps = part.split(':')
            if len(comps) < 2:
                    raise ZtreamyException('Event syntax error',
                                           'event_deserialize')
            header = comps[0].strip()
            value = part[len(comps[0]) + 1:].strip()
            self._update_header(header, value)
        if not self._header_complete:
            self._data = self._data[pos:]
            return None
        if not 'Body-Length' in self._event:
            body_length = 0
        else:
            body_length = int(self._event['Body-Length'])
        if (not 'Event-Id' in self._event
            or not 'Source-Id' in self._event
            or not 'Syntax' in self._event):
            raise ZtreamyException('Missing headers in event',
                                   'event_deserialize')
        end = pos + int(body_length)
        if end > len(self._data):
            self._data = self._data[pos:]
            return None
        body = self._data[pos:end]
        self._data = self._data[end:]
        if parse_body or self._event['Syntax'] in Event._always_parse:
            event = Event.create( \
                self._event.get('Source-Id'),
                self._event.get('Syntax'),
                body,
                event_id=self._event.get('Event-Id'),
                application_id=self._event.get('Application-Id'),
                aggregator_id=self._event.get('Aggregator-Ids', []),
                event_type=self._event.get('Event-Type'),
                timestamp=self._event.get('Timestamp'),
                extra_headers=self._extra_headers)
        else:
            event = Event( \
                self._event.get('Source-Id'),
                self._event.get('Syntax'),
                body,
                event_id=self._event.get('Event-Id'),
                application_id=self._event.get('Application-Id'),
                aggregator_id=self._event.get('Aggregator-Ids', []),
                event_type=self._event.get('Event-Type'),
                timestamp=self._event.get('Timestamp'),
                extra_headers=self._extra_headers)
        self._event_reset()
        return event

    def deserialize_file(self, file_, parse_body=True):
        """Generator that deserializes from a file-like object.

        It yields lists of event objects instead of single event objects.

        """
        while True:
            data = file_.read(8192)
            if data == '':
                break
            else:
                events = self.deserialize(data, parse_body=parse_body,
                                          complete=False)
                if events:
                    yield events

    def _update_header(self, header, value):
        if header not in Event.headers:
            self._extra_headers[header] = value
        elif header == 'Aggregator-Ids':
            self._event[header] = value.split(',')
        elif header not in self._event:
            self._event[header] = value
        else:
            raise ZtreamyException('Duplicate header in event',
                                   'event_deserialize')


class JSONDeserializer(object):
    def __init__(self):
        pass

    def deserialize(self, data, parse_body=True, complete=True):
        if not parse_body:
            raise ValueError('parse_body must be True in JSONDeserializer')
        if not complete:
            raise ValueError('data must be complete in JSONDeserializer')
        objects = json.loads(data)
        if not isinstance(objects, list):
            objects = [objects]
        return [JSONDeserializer._dict_to_event(e) for e in objects]

    @staticmethod
    def _dict_to_event(d):
        if (not 'Event-Id' in d
            or not 'Source-Id' in d
            or not 'Syntax' in d):
            raise ZtreamyException('Missing headers in event',
                                   'event_deserialize')
        if not 'Body' in d:
            raise ZtreamyException('Missing body in event',
                                   'event_deserialize')
        if isinstance(d['Body'], dict):
            body = d['Body']
        else:
            body = d['Body'].encode('utf-8')
        if ('Aggregator-Ids' in d
            and not isinstance(d['Aggregator-Ids'], list)):
            raise ZtreamyException('Incorrect Aggregator-Id data',
                                   'event_deserialize')
        extra_headers = {}
        for header in d:
            if not header in Event.headers and header != 'Body':
                extra_headers[header] = d[header]
        event = Event.create(d['Source-Id'],
                             d['Syntax'],
                             body,
                             event_id=d['Event-Id'],
                             application_id=d.get('Application-Id'),
                             aggregator_id=d.get('Aggregator-Ids', []),
                             event_type=d.get('Event-Type'),
                             timestamp=d.get('Timestamp'),
                             extra_headers=extra_headers)
        return event


class LDJSONDeserializer(JSONDeserializer):
    def __init__(self):
        super(LDJSONDeserializer, self).__init__()
        self._data = ''

    def deserialize(self, data, parse_body=True, complete=True):
        events = []
        if not parse_body:
            raise ValueError('parse_body must be True in JSONDeserializer')
        lines = data.split('\n')
        if self._data:
            # Some data is pending from the previous call
            lines[0] = self._data + lines[0]
        if lines:
            if lines[-1]:
                if complete:
                    raise ZtreamyException('Spurious data in the input event',
                                           'event_deserialize')
                else:
                    # Accumulate the data for the next call
                    self._data = lines[-1]
            del lines[-1]
            for line in lines:
                event_obj = json.loads(line)
                events.append(self._dict_to_event(event_obj))
        return events

    def reset(self):
        self._data = ''

    def data_is_pending(self):
        return self._data != ''


def single_event_from_file(filename):
    """Read a single event from the given filename.

    Raises ValueError if the file contains more than one event,
    ZtreamyException if syntax errors are found.
    IOError exceptions may also happen.

    """
    deserializer = Deserializer()
    with open(filename) as f:
        evs = deserializer.deserialize(f.read(), complete=True)
    if len(evs) == 1:
        event = evs[0]
    else:
        raise ValueError('More than one event read in buffer!')
    return event


class Event(object):
    """Generic event in the system.

    It is intended to be subclassed for application-specific types
    of events.

    """

    _subclasses = {}
    _always_parse = []
    headers = (
        'Event-Id',
        'Source-Id',
        'Syntax',
        'Application-Id',
        'Aggregator-Ids',
        'Event-Type',
        'Timestamp',
        'Body-Length',
        )
    _string_properties = (
        'event_id',
        'source_id',
        'syntax',
        'event_type',
        'timestamp',
    )

    @staticmethod
    def register_syntax(syntax, subclass, always_parse=False):
        """Registers a subclass of `Event` for a specific syntax.

        `subclass`should be a subclass of `Event`.  Overrides a
        previous registration for the same syntax.

        """
        assert issubclass(subclass, Event), \
            '{0} must be a subclass of Event'.format(subclass)
        Event._subclasses[syntax] = subclass
        if always_parse:
            Event._always_parse.append(syntax)

    @staticmethod
    def create(source_id, syntax, body, **kwargs):
        """Creates an instance of the appropriate subclass of `Event`.

        The subclass to use is the one registered for the syntax
        of the event (see 'register_syntax'). If no subclass has
        been registered for than syntax, an instance of 'Event'
        is returned instead.

        """
        if syntax in Event._subclasses:
            subclass = Event._subclasses[syntax]
        else:
            subclass = Event
        return subclass(source_id, syntax, body, **kwargs)

    def __init__(self, source_id, syntax, body, event_id=None,
                 application_id=None, aggregator_id=[], event_type=None,
                 timestamp=None, extra_headers=None):
        """Creates a new event.

        'body' must be the textual representation of the event, or an
        object providing that textual representation through 'str()'.

        When the created event has to be an instance of a specific
        subclass (e.g. an 'RDFEvent'), the static 'create()' method
        should be used instead.

        """
        if source_id is None:
            raise ValueError('Required event field missing: source_id')
        elif not syntax:
            raise ValueError('Required event field missing: syntax')
        self.event_id = event_id or ztreamy.random_id()
        self.source_id = source_id
        self.syntax = syntax
        self.body = body
        if aggregator_id is None:
            aggregator_id = []
        elif type(aggregator_id) is not list:
            self.aggregator_id = [str(aggregator_id)]
        else:
            self.aggregator_id = [str(e) for e in aggregator_id]
        self.event_type = event_type
        self._timestamp = timestamp or ztreamy.get_timestamp()
        self._time = None
        self.application_id = application_id
        self.extra_headers = {}
        if extra_headers is not None:
            # Do this in order to ensure type checking
            for header, value in extra_headers.iteritems():
                self.set_extra_header(header, value)

    def __setattr__(self, name, value):
        """Check the values of some event properties."""
        if (name in Event._string_properties
            and value is not None
            and not isinstance(value, basestring)):
            raise ValueError('String value expected for property ' + name)
        elif name == 'aggregator_id':
            if not isinstance(value, list):
                raise ValueError('List value expected for property ' + name)
            else:
                for item in value:
                    if not isinstance(item, basestring):
                        raise ValueError('Aggregator ids must be strings')
        super(Event, self).__setattr__(name, value)

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value):
        if value is None:
            raise ValueError('Attempting to set a None timestamp')
        self._timestamp = value
        self._time = None

    def time(self):
        """Event timestamp as a number of seconds since the epoch.

        The number of seconds is UTC-based.

        """
        if self._time is None:
            self._time = ztreamy.parse_timestamp(self.timestamp)
        return self._time

    def set_extra_header(self, header, value):
        """Adds an extra header to the event."""
        if (not isinstance(header, basestring)
            or not isinstance(value, basestring)):
            raise ValueError('String name/value expected for extra header')
        if header in Event.headers or header == 'Body':
            raise ValueError('Reserved extra heder name: ' + header)
        self.extra_headers[header] = value

    def append_aggregator_id(self, aggregator_id):
        """Appends a new aggregator id to the event."""
        self.aggregator_id.append(aggregator_id)

    def __str__(self):
        """Returns the string serialization of the event."""
        return self._serialize()

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    def serialize_body(self):
        """Returns a string representation of the body of the event.

        Raises a `ZtreamyException` if the body is None. This method
        should be overriden by subclasses in order to provide a
        syntax-specific serialization.

        """
        if self.body is not None:
            return str(self.body)
        else:
            raise ZtreamyException('Empty body in event', 'event_serialize')

    def as_dictionary(self, json=False):
        """Returns the event as a dictionary.

        The keys of the dictionary are the headers of the event and
        the special header 'Body'.

        If json is True, the body is represented also as JSON if
        possible. In addition, some headers are dropped.

        """
        data = {}
        data['Event-Id'] = self.event_id
        data['Source-Id'] = str(self.source_id)
        if self.application_id is not None:
            data['Application-Id'] = str(self.application_id)
        if self.aggregator_id != []:
            data['Aggregator-Ids'] = self.aggregator_id
        if self.event_type is not None:
            data['Event-Type'] = self.event_type
        if self.timestamp is not None:
            data['Timestamp'] = self.timestamp
        for header, value in self.extra_headers.iteritems():
            data[header] = value
        if json:
            body = self.body_as_json()
            syntax = self.syntax_as_json()
        else:
            body = None
            syntax = self.syntax
        data['Syntax'] = str(syntax)
        if body is None:
            data['Body'] = self.serialize_body().decode('utf-8')
        else:
            data['Body'] = body
        return data

    def as_json(self):
        return self.as_dictionary(json=True)

    def syntax_as_json(self):
        return self.syntax

    def body_as_json(self):
        return None

    def serialize_json(self):
        return json.dumps(self.as_json())

    def clone(self):
        return copy.copy(self)

    def _serialize(self):
        data = []
        data.append('Event-Id: ' + str(self.event_id))
        data.append('Source-Id: ' + str(self.source_id))
        data.append('Syntax: ' + str(self.syntax))
        if self.application_id is not None:
            data.append('Application-Id: ' + str(self.application_id))
        if self.aggregator_id != []:
            data.append('Aggregator-Ids: ' + ','.join( \
                                    [str(s) for s in self.aggregator_id]))
        if self.event_type is not None:
            data.append('Event-Type: ' + str(self.event_type))
        if self.timestamp is not None:
            data.append('Timestamp: ' + str(self.timestamp))
        for header, value in self.extra_headers.iteritems():
            data.append(str(header) + ': ' + str(value))
        serialized_body = self.serialize_body()
        data.append('Body-Length: ' + str(len(serialized_body)))
        data.append('')
        data.append(serialized_body)
        return '\r\n'.join(data)


class Command(Event):
    """Special event used for control at the middleware layer.

    These events are consumed by the event parser and never delivered
    to the rest of the application.

    """
    valid_commands = [
        'Test-Connection',
        'Event-Source-Started',
        'Event-Source-Finished',
        'Stream-Finished',
        ]

    def __init__(self, source_id, syntax, command, **kwargs):
        """Creates a new command event.

        `command` must be the textual representation of the command or
        provide that textual representation through `str()`. It will
        be the body of the event.

        """
        if syntax != 'ztreamy-command':
            raise ZtreamyException('Usupported syntax in Command',
                                   'programming')
        super(Command, self).__init__(source_id, syntax, command, **kwargs)
        self.command = command
        if not command in Command.valid_commands:
            raise ZtreamyException('Usupported command ' + command,
                                   'programming')

Event.register_syntax('ztreamy-command', Command, always_parse=True)


class TestEvent(Event):
    """Special event used for benchmarking and testing.

    The event encapsulates a sequence number and a timestamp.

    """
    def __init__(self, source_id, syntax, body, sequence_num=0, **kwargs):
        """Creates a new command event.

        ``sequence_num`` represents the sequence number (integer) of
        the event, and is transmitted in its body along with the
        timestamp. It is only used when body is None. If ``body`` is
        not None, the sequence number is read from ``body`` instead.

        """
        if syntax != 'ztreamy-test':
            raise ZtreamyException('Usupported syntax in TestEvent',
                                   'programming')
        super(TestEvent, self).__init__(source_id, syntax, '', **kwargs)
        if body is not None:
            parts = self.extra_headers['X-Float-Timestamp'].split('/')
            self.float_time = float(parts[1])
            self.sequence_num = int(parts[0])
        else:
            self.float_time = time.time()
            self.sequence_num = sequence_num
            self.extra_headers['X-Float-Timestamp'] = \
                str(sequence_num) + '/' + str(self.float_time)


Event.register_syntax('ztreamy-test', TestEvent, always_parse=True)


class JSONEvent(Event):
    """Event consisting of a JSON object."""

    supported_syntaxes = [ztreamy.json_media_type]

    def __init__(self, source_id, syntax, body, **kwargs):
        if not syntax in JSONEvent.supported_syntaxes:
            raise ZtreamyException('Usupported syntax in JSONEvent',
                                   'programming')
        if isinstance(body, basestring):
            body = self._parse_body(body)
        super(JSONEvent, self).__init__(source_id, syntax, body, **kwargs)

    def serialize_body(self):
        return json.dumps(self.body) + '\r\n\r\n'

    def body_as_json(self):
        return self.body

    def _parse_body(self, body):
        return json.loads(body)

for syntax in JSONEvent.supported_syntaxes:
    Event.register_syntax(syntax, JSONEvent, always_parse=True)


def create_command(source_id, command):
    return Command(source_id, 'ztreamy-command', command)

def parse_aggregator_ids(data):
    return [v.strip() for v in data.split(',') if v != '']
