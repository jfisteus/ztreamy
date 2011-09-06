""" A framework for transporting for semantic events."""

import random

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
    return hex(random.getrandbits(128))[2:-1]
