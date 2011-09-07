""" A framework for transporting for semantic events."""

import uuid
import time

from streamsem.utils.rfc3339 import rfc3339

mimetype_event = 'application/x-streamsem-event'

class StreamsemException(Exception):
    def __init__(self, message, error_type=None):
        Exception.__init__(self, message)
        self.code = self.error_type_code(error_type)

    def error_type_code(self, error_type):
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
        return rfc3339(date)
    else:
        return rfc3339(time.time())
