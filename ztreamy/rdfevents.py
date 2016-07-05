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
""" Code related to the modelling and manipulation of RDF events.

"""

import json

import rdflib

import ztreamy
from . import events
from ztreamy import ZtreamyException

class RDFEvent(events.Event):
    """Event consisting of an RDF body.

    Right now, only the Notation3 and JSON-LD serializations are allowed.

    """
    supported_syntaxes = ['text/n3', ztreamy.json_ld_media_type]

    def __init__(self, source_id, syntax, body, **kwargs):
        """Creates a new event.

        `body` must be the textual representation of the event or
        provide that textual representation through `str()`.

        """
        if not syntax in RDFEvent.supported_syntaxes:
            raise ZtreamyException('Usupported syntax in RDFEvent',
                                   'programming')
        super(RDFEvent, self).__init__(source_id, syntax, None, **kwargs)
        if isinstance(body, rdflib.Graph):
            self.body = body
        else:
            self.body = self._parse_body(body)

    def serialize_body(self):
        if self.syntax == 'text/n3':
            return self.body.serialize(format='n3')
        elif self.syntax == 'application/ld+json':
            return self.body.serialize(format='json-ld')
        else:
            raise ZtreamyException('Bad RDFEvent syntax', 'event_serialize')

    def syntax_as_json(self):
        return ztreamy.json_ld_media_type

    def body_as_json(self):
        json_obj = json.loads(self.body.serialize(format='json-ld'))
        return json_obj

    def _parse_body(self, body):
        if self.syntax == 'text/n3':
            return self._parse_body_rdflib(body, syntax='n3')
        elif self.syntax == 'application/ld+json':
            return self._parse_body_rdflib(body, syntax='json-ld')
        else:
            raise ZtreamyException('Unsupported syntax', 'event_syntax')

    def _parse_body_rdflib(self, body, syntax):
        g = rdflib.Graph()
        g.parse(data=body, format=syntax)
        return g

for syntax in RDFEvent.supported_syntaxes:
    events.Event.register_syntax(syntax, RDFEvent)
