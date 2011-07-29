""" Code related to the modelling and manipulation of events.

"""


class Event(object):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return str(self.message)
