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
import random
import string

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


class TestZlibDispatcher(unittest.TestCase):

    def test_subscribe(self):
        rnd = random.Random(45387)
        uncompressed_data = [
            _random_string(rnd, 1000),  #0
            _random_string(rnd, 6000),  #1  c2-3  c-4   c5-7
            _random_string(rnd, 7000),  #2   7000
            _random_string(rnd, 4000),  #3  11000  4000
            _random_string(rnd, 9000),  #4  20000 13000
            _random_string(rnd, 9000),  #5  29000 22000
            _random_string(rnd, 4000),  #6  33000 26000  4000
            _random_string(rnd, 10000), #7        36000 14000
            _random_string(rnd, 5000),  #8              19000
            _random_string(rnd, 4000),  #9              23000
        ]
        stream = _MockStream('test/stream/path')
        props = ClientPropertiesFactory.create( \
                                streaming=True,
                                encoding=ClientProperties.ENCODING_ZLIB)
        d = ztreamy.dispatchers.ZlibDispatcher(stream, props)
        clients = [_MockClient(properties=props) for _ in range(8)]
        d.dispatch(_MockEventsPack(uncompressed_data[0]))
        self.assertEqual(len(d.groups), 1)
        d.subscribe(clients[0])
        d.subscribe(clients[1])
        d.dispatch(_MockEventsPack(uncompressed_data[1]))
        self.assertEqual(len(d.groups), 1)
        d.subscribe(clients[2])
        d.subscribe(clients[3])
        d.dispatch(_MockEventsPack(uncompressed_data[2]))
        self.assertEqual(len(d.groups), 2)
        d.subscribe(clients[4])
        d.dispatch(_MockEventsPack(uncompressed_data[3]))
        self.assertEqual(len(d.groups), 3)
        d.dispatch(_MockEventsPack(uncompressed_data[4]))
        self.assertEqual(len(d.groups), 3)
        d.dispatch(_MockEventsPack(uncompressed_data[5]))
        d.subscribe(clients[5])
        d.subscribe(clients[6])
        d.subscribe(clients[7])
        d.dispatch(_MockEventsPack(uncompressed_data[6]))
        self.assertEqual(len(d.groups), 4)
        d.dispatch(_MockEventsPack(uncompressed_data[7]))
        self.assertEqual(len(d.groups), 3) # merge c0-3
        d.dispatch(_MockEventsPack(uncompressed_data[8]))
        self.assertEqual(len(d.groups), 2) # merge c0-4
        d.dispatch(_MockEventsPack(uncompressed_data[9]))
        self.assertEqual(len(d.groups), 2)
        data_c0_1 = ''.join(uncompressed_data[1:])
        self.assertEqual(clients[0].unzlib_sent_data(), data_c0_1)
        self.assertEqual(clients[1].unzlib_sent_data(), data_c0_1)
        data_c2_3 = ''.join(uncompressed_data[2:])
        self.assertEqual(clients[2].unzlib_sent_data(), data_c2_3)
        self.assertEqual(clients[3].unzlib_sent_data(), data_c2_3)
        data_c4 = ''.join(uncompressed_data[3:])
        self.assertEqual(clients[4].unzlib_sent_data(), data_c4)
        data_c5_7 = ''.join(uncompressed_data[6:])
        self.assertEqual(clients[5].unzlib_sent_data(), data_c5_7)
        self.assertEqual(clients[6].unzlib_sent_data(), data_c5_7)
        self.assertEqual(clients[7].unzlib_sent_data(), data_c5_7)

    def test_unsubscribe(self):
        rnd = random.Random(45387)
        uncompressed_data = [
            _random_string(rnd, 1000),  #0
            _random_string(rnd, 6000),  #1  c2-3  c-4   c5-7
            _random_string(rnd, 7000),  #2   7000
            _random_string(rnd, 4000),  #3  11000  4000
            _random_string(rnd, 9000),  #4  20000 13000
            _random_string(rnd, 9000),  #5  29000 22000
            _random_string(rnd, 4000),  #6  33000 26000  4000
            _random_string(rnd, 10000), #7        36000 14000
            _random_string(rnd, 5000),  #8              19000
            _random_string(rnd, 4000),  #9              23000
        ]
        stream = _MockStream('test/stream/path')
        props = ClientPropertiesFactory.create( \
                                streaming=True,
                                encoding=ClientProperties.ENCODING_ZLIB)
        d = ztreamy.dispatchers.ZlibDispatcher(stream, props)
        clients = [_MockClient(properties=props) for _ in range(8)]
        d.dispatch(_MockEventsPack(uncompressed_data[0]))
        self.assertEqual(len(d.groups), 1)
        d.subscribe(clients[0])
        d.subscribe(clients[1])
        d.dispatch(_MockEventsPack(uncompressed_data[1]))
        self.assertEqual(len(d.groups), 1)
        d.subscribe(clients[2])
        d.subscribe(clients[3])
        d.unsubscribe(clients[1])
        d.dispatch(_MockEventsPack(uncompressed_data[2]))
        self.assertEqual(len(d.groups), 2)
        d.subscribe(clients[4])
        d.dispatch(_MockEventsPack(uncompressed_data[3]))
        self.assertEqual(len(d.groups), 3)
        d.unsubscribe(clients[2])
        d.unsubscribe(clients[3])
        d.dispatch(_MockEventsPack(uncompressed_data[4]))
        self.assertEqual(len(d.groups), 2)
        d.dispatch(_MockEventsPack(uncompressed_data[5]))
        d.subscribe(clients[5])
        d.subscribe(clients[6])
        d.subscribe(clients[7])
        d.dispatch(_MockEventsPack(uncompressed_data[6]))
        self.assertEqual(len(d.groups), 3)
        d.dispatch(_MockEventsPack(uncompressed_data[7]))
        self.assertEqual(len(d.groups), 3) # merge c0-3
        d.dispatch(_MockEventsPack(uncompressed_data[8]))
        self.assertEqual(len(d.groups), 2) # merge c0-4
        d.dispatch(_MockEventsPack(uncompressed_data[9]))
        self.assertEqual(len(d.groups), 2)
        data_c0 = ''.join(uncompressed_data[1:])
        self.assertEqual(clients[0].unzlib_sent_data(), data_c0)
        data_c1 = ''.join(uncompressed_data[1:2])
        self.assertEqual(clients[1].unzlib_sent_data(), data_c1)
        data_c2_3 = ''.join(uncompressed_data[2:4])
        self.assertEqual(clients[2].unzlib_sent_data(), data_c2_3)
        self.assertEqual(clients[3].unzlib_sent_data(), data_c2_3)
        data_c4 = ''.join(uncompressed_data[3:])
        self.assertEqual(clients[4].unzlib_sent_data(), data_c4)
        data_c5_7 = ''.join(uncompressed_data[6:])
        self.assertEqual(clients[5].unzlib_sent_data(), data_c5_7)
        self.assertEqual(clients[6].unzlib_sent_data(), data_c5_7)
        self.assertEqual(clients[7].unzlib_sent_data(), data_c5_7)


class _MockStream(object):
    def __init__(self, path):
        self.path = path


class _MockEventsPack(ztreamy.dispatchers.EventsPack):
    def __init__(self, events):
        super(_MockEventsPack, self).__init__(events)

    def serialize(self, serialization):
        return ''.join(str(event) for event in self.events)


class _MockClient(object):
    def __init__(self, properties=None):
        self.is_fresh = True
        self.properties = properties
        self.sent_data = ''

    def send(self, data, flush=True):
        self.sent_data += data
        if data:
            self.is_fresh = False

    def unzlib_sent_data(self):
        return _zlib_decompress_part(self.sent_data)


def _zlib_decompress_part(data):
    decompressor = zlib.decompressobj()
    decompressed = decompressor.decompress(data)
    while decompressor.unconsumed_tail:
        decompressed += decompressor.decompress(decompressor.unconsumed_tail)
    if decompressor.unused_data:
        raise ValueError('Unused data in zlib decompression')
    return decompressed

_rnd_chars = (string.ascii_lowercase
              + 12 * ' ' + 5 * 'a' + 6 * 'e' + 2 * 'i' + 3 * 'o' + 2 * 'u')
def _random_string(random_obj, length):
    return ''.join(random_obj.choice(_rnd_chars) for _ in range(length))
