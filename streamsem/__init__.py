""" A framework for transporting for semantic events."""

class StreamsemException(Exception):
    def __init__(self, message, error_type=None):
        Exception.__init__(self, message)
        self.code = self.error_type_code(error_type)

    def error_type_code(self, error_type):
        if error_type == 'event_syntax':
            return 1
        else:
            return 0
