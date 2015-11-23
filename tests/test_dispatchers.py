# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2014-2015 Jesus Arias Fisteus
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

import unittest
import zlib

import ztreamy.dispatchers
from ztreamy.dispatchers import (SubscriptionGroup, ClientPropertiesFactory,
                                 ClientProperties, )


class TestSubscriptionGroup(unittest.TestCase):

    def test_zlib_fresh_client(self):
        props = ClientPropertiesFactory.create( \
                                streaming=True,
                                encoding=ClientProperties.ENCODING_ZLIB)
        group = SubscriptionGroup(props)
        serialization = props.serialization
        client = _MockClient()
        group.subscribe(client)
        events1 = _MockEventsPack(['This is event 1', 'Another event'])
        events2 = _MockEventsPack(['This is event 3', 'And the last event'])
        group.dispatch(events1)
        self.assertEqual(_zlib_decompress_part(client.sent_data),
                         events1.serialize(serialization))
        group.dispatch(events2)
        self.assertEqual(_zlib_decompress_part(client.sent_data),
                         (events1.serialize(serialization)
                          + events2.serialize(serialization)))

    def test_zlib_client_with_last_seen(self):
        props = ClientPropertiesFactory.create( \
                                streaming=True,
                                encoding=ClientProperties.ENCODING_ZLIB)
        group = SubscriptionGroup(props)
        serialization = props.serialization
        client = _MockClient()
        initial_data = 'This is a pack of initial data.'
        client.send(ztreamy.dispatchers.compress_zlib(initial_data))
        group.subscribe(client)
        events1 = _MockEventsPack(['This is event 1', 'Another event'])
        events2 = _MockEventsPack(['This is event 3', 'And the last event'])
        self.assertEqual(_zlib_decompress_part(client.sent_data),
                         initial_data)
        group.dispatch(events1)
        self.assertEqual(_zlib_decompress_part(client.sent_data),
                         (initial_data
                          + events1.serialize(serialization)))

        group.dispatch(events2)
        self.assertEqual(_zlib_decompress_part(client.sent_data),
                         (initial_data
                          + events1.serialize(serialization)
                          + events2.serialize(serialization)))


class _MockEventsPack(ztreamy.dispatchers.EventsPack):
    def __init__(self, events):
        super(_MockEventsPack, self).__init__(events)

    def serialize(self, serialization):
        return ''.join(str(event) for event in self.events)


class _MockClient(object):
    def __init__(self):
        self.is_fresh = True
        self.sent_data = ''

    def send(self, data, flush=True):
        self.sent_data += data
        if data:
            self.is_fresh = False

def _zlib_decompress_part(data):
    decompressor = zlib.decompressobj()
    decompressed = decompressor.decompress(data)
    while decompressor.unconsumed_tail:
        decompressed += decompressor.decompress(decompressor.unconsumed_tail)
    if decompressor.unused_data:
        raise ValueError('Unused data in zlib decompression')
    return decompressed
