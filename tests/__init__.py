import unittest


def get_tests():
    return full_suite()


def full_suite():
    from .test_events import TestEvent
    from .test_server import TestServer

    eventssuite = unittest.TestLoader().loadTestsFromTestCase(TestEvent)
    serversuite = unittest.TestLoader().loadTestsFromTestCase(TestServer)

    return unittest.TestSuite([eventssuite, serversuite])
