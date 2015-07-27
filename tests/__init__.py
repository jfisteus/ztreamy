import unittest


def get_tests():
    return full_suite()


def full_suite():
    from .test_events import TestEvent
    from .test_server import TestServer
    from .test_filters import TestEventTypeFilter

    eventssuite = unittest.TestLoader().loadTestsFromTestCase(TestEvent)
    serversuite = unittest.TestLoader().loadTestsFromTestCase(TestServer)
    filterssuite = unittest.TestLoader()\
        .loadTestsFromTestCase(TestEventTypeFilter)

    return unittest.TestSuite([eventssuite, serversuite, filterssuite])
