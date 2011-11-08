""" Code related to the modelling and manipulation of RDF events.

"""

from rdflib.graph import Graph

import streamsem.events as events
from streamsem import StreamsemException

class RDFEvent(events.Event):

    supported_syntaxes = ['n3']

    def __init__(self, source_id, syntax, body, **kwargs):
        """Creates a new event.

        `body` must be the textual representation of the event or
        provide that textual representation through `str()`.

        """
        if not syntax in RDFEvent.supported_syntaxes:
            raise StreamsemException('Usupported syntax in RDFEvent',
                                     'programming')

        super(RDFEvent, self).__init__(source_id, syntax, None, **kwargs)
        if isinstance(body, Graph):
            self.body = body
        else:
            self.body = self._parse_body(body)

    def serialize_body(self):
        if self.syntax == 'n3':
            return self.body.serialize(format='n3')
        else:
            raise StreamsemException('Bad RDFEvent syntax', 'event_serialize')

    def _parse_body(self, body):
        if self.syntax == 'n3':
            return self._parse_body_rdflib(body, syntax='n3')
        else:
            raise StreamsemException('Unsupported syntax',
                                               'event_syntax')

    def _parse_body_rdflib(self, body, syntax):
        g = Graph()
        g.parse(data=body, format=syntax)
        return g

for syntax in RDFEvent.supported_syntaxes:
    events.Event.register_syntax(syntax, RDFEvent)
