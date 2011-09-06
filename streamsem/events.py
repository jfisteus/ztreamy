""" Code related to the modelling and manipulation of events.

"""

from rdflib.graph import Graph

import streamsem

class Event(object):
    def __init__(self, source_id, syntax, message, aggregator_id=[],
                 event_type=None, timestamp=None):
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
        self.timestamp = timestamp
        self.message = self.parse_message(message)

    def append_aggregator_id(self, aggregator_id):
        """Appends a new aggregator id to the event."""
        self.aggregator_id.append(aggregator_id)

    def parse_message(self, message):
        if self.syntax == 'n3':
            return self.parse_message_rdflib(message, syntax='n3')
        elif self.syntax == 'text/plain':
            return message
        else:
            raise StreamsemException('Unsupported syntax', 'event_syntax')

    def parse_message_rdflib(self, message, syntax):
        g = Graph()
        g.parse(data=message, format=syntax)
        return g

    def serialize(self):
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
        if self.syntax == 'n3':
            data.append(self.message.serialize(format='n3'))
        elif self.syntax == 'text/plain':
            data.append(self.message)
        else:
            raise StreamsemException('Unsupported syntax', 'event_syntax')
        return '\n'.join(data)

    def __str__(self):
        return self.serialize()

    def _parse_syntax(self, syntax):
        if syntax == 'n3' or syntax == 'text/plain':
            return syntax
        else:
            raise StreamsemException('Unknown syntax', 'event_syntax')

def deserialize_event(data):
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
            if len(comps) != 2:
                raise StreamsemException('Event syntax error',
                                         'event_deserialize')
            header = comps[0].strip()
            value = comps[1].strip()
            if header == 'Event-Id':
                if event_id is None:
                    event_id = value
                else:
                    raise StreamsemException('Duplicate header in event',
                                             'event_deserialize')
            if header == 'Source-Id':
                if source_id is None:
                    source_id = value
                else:
                    raise StreamsemException('Duplicate header in event',
                                             'event_deserialize')
            elif header == 'Syntax':
                if syntax is None:
                    syntax = value
                else:
                    raise StreamsemException('Duplicate header in event',
                                             'event_deserialize')
            elif header == 'Aggregator-Ids':
                if aggregator_id == []:
                    aggregator_id = [v.strip() for v in value.split(',')]
                else:
                    raise StreamsemException('Duplicate header in event',
                                             'event_deserialize')
            elif header == 'Event-Type':
                if event_type is None:
                    event_type = value
                else:
                    raise StreamsemException('Duplicate header in event',
                                             'event_deserialize')
            elif header == 'Timestamp':
                if timestamp is None:
                    timestamp = value
                else:
                    raise StreamsemException('Duplicate header in event',
                                             'event_deserialize')
            num_headers += 1
        else:
            break
    message = '\n'.join(parts[num_headers + 1:])
    if event_id is None or source_id is None or syntax is None:
        raise StreamsemException('Missing headers in event',
                                 'event_deserialize')
    return Event(source_id, syntax, message, aggregator_id=aggregator_id,
                 event_type=event_type, timestamp=timestamp)
