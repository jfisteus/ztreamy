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
# <http://www.gnu.org/licenses/>.
#
""" A framework for publishing semantic events on the Web."""

import uuid
import time

import streamsem.utils.rfc3339

mimetype_event = 'application/x-streamsem-event'


class StreamsemException(Exception):
    """The type of the exceptions normally used in the framework."""
    def __init__(self, message, error_type=None):
        Exception.__init__(self, message)
        self.code = StreamsemException._error_type_code(error_type)

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
        return streamsem.utils.rfc3339.rfc3339(date)
    else:
        return streamsem.utils.rfc3339.rfc3339(time.time())

_date_format = "%Y-%m-%dT%H:%M:%S"

def rfc3339_as_time(timestamp):
    """Returns the given RFC 3339 timestamp as a seconds since the epoch value.

    Note that the timezone information from the timestamp is lost.

    """
    return time.mktime(time.strptime(timestamp[:-6], _date_format))

# Imports of the main classes of the API provided by the framework,
# in order to make them available in the "streamsem" namespace.
#
from server import StreamServer, RelayServer
from client import Client, AsyncStreamingClient
from events import Deserializer, Event
from rdfevents import RDFEvent
from filters import Filter, SourceFilter, ApplicationFilter, SimpleTripleFilter
