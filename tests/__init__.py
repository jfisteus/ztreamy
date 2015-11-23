import unittest


def get_tests():
    return full_suite()


def full_suite():
    from .test_events import TestEvent
    from .test_server import TestServer
    from .test_filters import TestEventTypeFilter
    from .test_dispatchers import TestSubscriptionGroup

    events_suite = unittest.TestLoader().loadTestsFromTestCase(TestEvent)
    server_suite = unittest.TestLoader().loadTestsFromTestCase(TestServer)
    filters_suite = unittest.TestLoader()\
        .loadTestsFromTestCase(TestEventTypeFilter)
    dispatchers_suite = unittest.TestLoader()\
        .loadTestsFromTestCase(TestSubscriptionGroup)

    return unittest.TestSuite([events_suite, server_suite, filters_suite,
                               dispatchers_suite])
