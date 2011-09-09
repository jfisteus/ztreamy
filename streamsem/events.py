""" Code related to the modelling and manipulation of events.

"""

import streamsem
from streamsem import StreamsemException

class Event(object):
    """Represents a generic event in the system.

    It is intended to be subclassed for application-specific types
    of events.

    """

    _subclasses = {}

    @staticmethod
    def register_syntax(syntax, subclass):
        """Registers a subclass of `Event` for a specific syntax.

        `subclass`should be a subclass of `Event`.  Overrides a
        previous registration for the same syntax.

        """
        assert issubclass(subclass, Event), \
            '{0} must be a subclass of Event'.format(subclass)
        Event._subclasses[syntax] = subclass

    @staticmethod
    def create(source_id, syntax, body, **kwargs):
        """Creates an instance of the appropriate subclass of `Event`.

        The subclass to use is the one registered for the syntax
        of the event (see `register_syntax`). If no subclass has
        been registered for than syntax, an instance of `Event`
        is returned instead.

        """
        if syntax in Event._subclasses:
            subclass = Event._subclasses[syntax]
        else:
            subclass = Event
        return subclass(source_id, syntax, body, **kwargs)

    def __init__(self, source_id, syntax, body, application_id=None,
                 aggregator_id=[], event_type=None, timestamp=None):
        """Creates a new event.

        `body` must be the textual representation of the event or
        provide that textual representation through `str()`.

        """
        self.event_id = streamsem.random_id()
        self.source_id = source_id
        self.syntax = syntax
        self.body = body
        if aggregator_id is None:
            aggregator_id = []
        else:
            if type(aggregator_id) is not list:
                self.aggregator_id = [str(aggregator_id)]
            else:
                self.aggregator_id = [str(e) for e in aggregator_id]
        self.event_type = event_type
        self.timestamp = timestamp or streamsem.get_timestamp()
        self.application_id = application_id

    def append_aggregator_id(self, aggregator_id):
        """Appends a new aggregator id to the event."""
        self.aggregator_id.append(aggregator_id)

    def __str__(self):
        """Returns the string serialization of the event."""
        return self._serialize()

    @staticmethod
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
        application_id = None
        aggregator_id = []
        event_type = None
        timestamp = None
        num_headers = 0
        for part in parts:
            if part == '':
                break
            comps = part.split(':')
            if len(comps) < 2:
                raise StreamsemException('Event syntax error',
                                         'event_deserialize')
            header = comps[0].strip()
            value = part[len(comps[0]) + 1:].strip()
            if header == 'Event-Id':
                if event_id is None:
                    event_id = value
                else:
                    raise StreamsemException('Duplicate header in event',
                                             'event_deserialize')
            elif header == 'Source-Id':
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
            elif header == 'Application-Id':
                if application_id is None:
                    application_id = value
                else:
                    raise StreamsemException('Duplicate header in event',
                                             'event_deserialize')
            elif header == 'Aggregator-Ids':
                if aggregator_id == []:
                    aggregator_id = parse_aggregator_id(value)
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
        body = '\n'.join(parts[num_headers + 1:])
        if event_id is None or source_id is None or syntax is None:
            raise StreamsemException('Missing headers in event',
                                     'event_deserialize')
        if parse_body:
            return Event.create(source_id, syntax, body,
                                application_id=application_id,
                                aggregator_id=aggregator_id,
                                event_type=event_type,
                                timestamp=timestamp)
        else:
            return Event(source_id, syntax, body,
                         application_id=application_id,
                         aggregator_id=aggregator_id,
                         event_type=event_type,
                         timestamp=timestamp)

    def serialize_body(self):
        """Returns a string representation of the body of the event.

        Raises a `StreamsemException` if the body is None. This method
        should be overriden by subclasses in order to provide a
        syntax-specific serialization.

        """
        if self.body is not None:
            return str(self.body)
        else:
            raise StreamsemException('Empty body in event', 'even_serialize')

    def _serialize(self):
        data = []
        data.append('Event-Id: ' + self.event_id)
        data.append('Source-Id: ' + str(self.source_id))
        data.append('Syntax: ' + str(self.syntax))
        if self.application_id is not None:
            data.append('Application-Id: ' + str(self.application_id))
        if self.aggregator_id != []:
            data.append('Aggregator-Ids: ' + ','.join(self.aggregator_id))
        if self.event_type is not None:
            data.append('Event-Type: ' + str(self.event_type))
        if self.timestamp is not None:
            data.append('Timestamp: ' + str(self.timestamp))
        data.append('')
        data.append(self.serialize_body())
        return '\n'.join(data)


def parse_aggregator_id(data):
    return [v.strip() for v in data.split(',') if v != '']
