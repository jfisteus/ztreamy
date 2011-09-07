""" Code related to the modelling and manipulation of events.

"""

from rdflib.graph import Graph

import streamsem

class Event(object):
    def __init__(self, source_id, syntax, body, aggregator_id=[],
                 event_type=None, timestamp=None, parse_body=True):
        """Creates a new event.

        parse_body -- If True (default value) the body is parsed.  If
        False, the body is stored as it is, without parsing.

        """
        self.event_id = streamsem.random_id()
        self.source_id = source_id
        self.syntax = self._parse_syntax(syntax)
        if aggregator_id is None:
            aggregator_id = []
        else:
            if type(aggregator_id) is not list:
                self.aggregator_id = [str(aggregator_id)]
            else:
                self.aggregator_id = [str(e) for e in aggregator_id]
        self.event_type = event_type
        if timestamp is not None:
            self.timestamp = timestamp
        else:
            self.timestamp = streamsem.get_timestamp()
        if parse_body:
            self.body = self._parse_body_internal(body)
            self.internal_syntax = syntax
        else:
            self.body = str(body)
            self.internal_syntax = 'unparsed'

    def append_aggregator_id(self, aggregator_id):
        """Appends a new aggregator id to the event."""
        self.aggregator_id.append(aggregator_id)

    def parse_body(self):
        """Parses the event body if it was not parsed before."""
        if self.internal_syntax == 'unparsed':
            self.body = self._parse_body_internal(self.body)
            self.internal_syntax = self.syntax

    def __str__(self):
        return self._serialize()

    def _parse_body_internal(self, body):
        if self.syntax == 'n3':
            return self._parse_body_rdflib(body, syntax='n3')
        elif self.syntax == 'text/plain':
            return body
        else:
            raise streamsem.StreamsemException('Unsupported syntax',
                                               'event_syntax')

    def _parse_body_rdflib(self, body, syntax):
        g = Graph()
        g.parse(data=body, format=syntax)
        return g

    def _serialize(self):
        data = []
        data.append('Event-Id: ' + self.event_id)
        data.append('Source-Id: ' + str(self.source_id))
        data.append('Syntax: ' + str(self.syntax))
        if self.aggregator_id != []:
            data.append('Aggregator-Ids: ' + ','.join(self.aggregator_id))
        if self.event_type is not None:
            data.append('Event-Type: ' + str(self.event_type))
        if self.timestamp is not None:
            data.append('Timestamp: ' + str(self.timestamp))
        data.append('')
        if self.internal_syntax == 'n3':
            data.append(self.body.serialize(format='n3'))
        elif self.internal_syntax == 'text/plain':
            data.append(self.body)
        elif self.internal_syntax == 'unparsed':
            data.append(self.body)
        else:
            raise streamsem.StreamsemException('Unsupported syntax',
                                               'event_syntax')
        return '\n'.join(data)

    def _parse_syntax(self, syntax):
        if syntax == 'n3' or syntax == 'text/plain':
            return syntax
        else:
            raise streamsem.StreamsemException('Unknown syntax',
                                               'event_syntax')

def deserialize(data, parse_body=True):
    """Deserializes and returns an event from the given string.

    `data` -- the string representing the event

    `parse_body` -- if True, the body of the event is parsed according
    to its type. If not, it is stored just as a string in the event
    object.

    """
    parts = data.split('\n')
    event_id = None
    source_id = None
    syntax = None
    aggregator_id = []
    event_type = None
    timestamp = None
    num_headers = 0
    for part in parts:
        if part != '':
            comps = part.split(':')
            if len(comps) < 2:
                raise streamsem.StreamsemException('Event syntax error',
                                                   'event_deserialize')
            header = comps[0].strip()
            value = part[len(comps[0]) + 1:].strip()
            if header == 'Event-Id':
                if event_id is None:
                    event_id = value
                else:
                    raise streamsem.StreamsemException(
                        'Duplicate header in event', 'event_deserialize')
            if header == 'Source-Id':
                if source_id is None:
                    source_id = value
                else:
                    raise streamsem.StreamsemException(
                        'Duplicate header in event', 'event_deserialize')
            elif header == 'Syntax':
                if syntax is None:
                    syntax = value
                else:
                    raise streamsem.StreamsemException(
                        'Duplicate header in event', 'event_deserialize')
            elif header == 'Aggregator-Ids':
                if aggregator_id == []:
                    aggregator_id = parse_aggregator_id(value)
                else:
                    raise streamsem.StreamsemException(
                        'Duplicate header in event', 'event_deserialize')
            elif header == 'Event-Type':
                if event_type is None:
                    event_type = value
                else:
                    raise streamsem.StreamsemException(
                        'Duplicate header in event', 'event_deserialize')
            elif header == 'Timestamp':
                if timestamp is None:
                    timestamp = value
                else:
                    raise streamsem.StreamsemException(
                        'Duplicate header in event', 'event_deserialize')
            num_headers += 1
        else:
            break
    body = '\n'.join(parts[num_headers + 1:])
    if event_id is None or source_id is None or syntax is None:
        raise streamsem.StreamsemException('Missing headers in event',
                                           'event_deserialize')
    return Event(source_id, syntax, body, aggregator_id=aggregator_id,
                 event_type=event_type, timestamp=timestamp,
                 parse_body=parse_body)

def parse_aggregator_id(data):
    return [v.strip() for v in data.split(',') if v != '']
