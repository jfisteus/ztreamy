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
""" A framework for publishing semantic events on the Web."""

import uuid
import time
from urlparse import urlparse
import json
import datetime

import dateutil.tz
import dateutil.parser

import ztreamy.utils.rfc3339

stream_media_type = 'application/ztreamy-stream'
event_media_type = 'application/ztreamy-event'
json_media_type = 'application/json'
ldjson_media_type = 'application/x-ldjson'
json_ld_media_type = 'application/ld+json'

SERIALIZATION_NONE = 0
SERIALIZATION_ZTREAMY = 1
SERIALIZATION_JSON = 2
SERIALIZATION_LDJSON = 3
SERIALIZATIONS = (
    SERIALIZATION_NONE,
    SERIALIZATION_ZTREAMY,
    SERIALIZATION_JSON,
    SERIALIZATION_LDJSON,
)


class ZtreamyException(Exception):
    """The type of the exceptions normally used in the framework."""
    def __init__(self, message, error_type=None):
        Exception.__init__(self, message)
        self.code = ZtreamyException._error_type_code(error_type)

    @staticmethod
    def _error_type_code(error_type):
        if error_type == 'event_syntax':
            return 1
        else:
            return 0


def random_id():
    """Returns a random id.

       The hexadecimal representation of a 128 bit random number is
       returned as a string.

    """
    return str(uuid.uuid4())

def get_timestamp(date=None):
    """Returns a string with 'time' formatted according to :RFC:`3339`.

    `date` -- a `datetime.datetime`, `datetime.date` or a timestamp as
    returned by `time.time()`. If `date` is None or not provided,
    current time is used instead.

    """
    if date is not None:
        return ztreamy.utils.rfc3339.rfc3339(date)
    else:
        return ztreamy.utils.rfc3339.rfc3339(time.time())

_tz_utc = dateutil.tz.tz.tzutc()
_epoc_utc = datetime.datetime(1970, 1, 1, tzinfo=_tz_utc)

def parse_timestamp(timestamp, default_tz=None):
    """Returns the given timestamp string as seconds since the epoch.

    It accepts any date format accepted by the dateutil package.
    For example, RFC 3339 is one of them.

    The timestamp is returned for the UTC time zone as a floating point
    number. If the input timestamp is in other timezone, this function
    takes care of the needed conversions.

    The timestamp should come with time zone information. If not, and a
    'default_tz' argument is passed, the timestamp is assumed to be in
    that zone (see timezone objets in dateutil.tz). However, if no
    'default_tz' is given, an ZtreamyException will be rised.

    """
    try:
        t = dateutil.parser.parse(timestamp)
    except ValueError:
            raise ZtreamyException(('Incorrect timestamp: {}. '
                                    '; RFC3339 timestamps and other '
                                    'commonly used formats are accepted')\
                                    .format(timestamp))
    if not t.tzinfo:
        if default_tz is not None:
            t = t.replace(tzinfo=default_tz)
        else:
            raise ZtreamyException('Timestamps without timezone '
                                   'are not accepted')
    t_utc = t.astimezone(_tz_utc)
    return (t_utc - _epoc_utc).total_seconds()

def serialize_events(events, serialization=SERIALIZATION_ZTREAMY):
    """Returns a string with the serialization of the events.

    'events' is a list of events. 'serrialization' is a string with
    the desired serialization format. Currently only 'ztreamy' and
    'json' are supported. The default value is 'ztreamy'.

    """
    if serialization == SERIALIZATION_ZTREAMY:
        formatter_func = lambda event: event._serialize
    elif serialization == SERIALIZATION_JSON:
        return serialize_events_json(events)
    elif serialization == SERIALIZATION_LDJSON:
        return serialize_events_ldjson(events)
    else:
        raise ZtreamyException('Unknown serialization format', 'send_event')
    data = []
    for e in events:
        data.append(formatter_func(e)())
    return ''.join(data)

def serialize_events_json(events):
    data = [e.as_json() for e in events]
    return json.dumps(data)

def serialize_events_ldjson(events):
    if events:
        data = [e.serialize_json() for e in events]
        return '\r\n'.join(item for item in data) + '\r\n'
    else:
        return ''

def split_url(url):
    """Returns the URL splitted in components.

    Returns the tuple (scheme, hostname, port, path). `path` includes
    the query string if present in the URL.

    """
    url_parts = urlparse(url)
    port = url_parts.port or 80
    path = url_parts.path
    if url_parts.query:
        path += '?' + url_parts.query
    return url_parts.scheme, url_parts.hostname, port, path


# Imports of the main classes of the API provided by the framework,
# in order to make them available in the "ztreamy" namespace.
#
from events import Deserializer, JSONDeserializer, Event, Command, JSONEvent
from rdfevents import RDFEvent
from filters import (Filter, SourceFilter, ApplicationFilter,
                     SimpleTripleFilter, VocabularyFilter,
                     SPARQLFilter, TripleFilter)
from server import StreamServer, Stream, RelayStream
from client import (Client, AsyncStreamingClient, SynchronousClient,
                    EventPublisher, SynchronousEventPublisher,
                    LocalClient, LocalEventPublisher)
