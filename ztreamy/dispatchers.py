# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2015 Jesus Arias Fisteus
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
"""Dispatcher used by the server to send the data to its clients.

They are intended to be used by the server.

"""
from __future__ import print_function, unicode_literals

import zlib
import gzip
import StringIO
import time
import logging

import ztreamy
from . import events


class ClientProperties(object):
    ENCODING_PLAIN = 0
    ENCODING_ZLIB = 1
    ENCODING_GZIP = 2
    ENCODINGS = (
        ENCODING_PLAIN,
        ENCODING_ZLIB,
        ENCODING_GZIP,
    )

    @staticmethod
    def create():
        pass

    def __init__(self, streaming, serialization, encoding, local, priority):
        self.__streaming = streaming
        self.__serialization = serialization
        self.__encoding = encoding
        self.__local = local
        self.__priority = priority

    @property
    def streaming(self):
        return self.__streaming

    @property
    def serialization(self):
        return self.__serialization

    @property
    def encoding(self):
        return self.__encoding

    @property
    def local(self):
        return self.__local

    @property
    def priority(self):
        return self.__priority

    def __str__(self):
        parts = []
        if self.streaming:
            parts.append('streaming')
        else:
            parts.append('polling')
        if self.serialization == ztreamy.SERIALIZATION_JSON:
            parts.append('json')
        elif self.serialization == ztreamy.SERIALIZATION_LDJSON:
            parts.append('ldjson')
        if self.encoding == ClientProperties.ENCODING_ZLIB:
            parts.append('zlib')
        elif self.encoding == ClientProperties.ENCODING_GZIP:
            parts.append('gzip')
        if self.local:
            parts.append('local')
        if self.priority:
            parts.append('priority')
        return '-'.join(parts)


class ClientPropertiesFactory(object):
    instances = {}

    @staticmethod
    def create(streaming=False,
               serialization=ztreamy.SERIALIZATION_ZTREAMY,
               encoding=ClientProperties.ENCODING_PLAIN,
               local=False,
               priority=False):
        if streaming not in (True, False):
            raise ValueError('Bad streaming value: ' + str(streaming))
        if serialization not in ztreamy.SERIALIZATIONS:
            raise ValueError('Bad serialization value: ' + str(serialization))
        if encoding not in ClientProperties.ENCODINGS:
            raise ValueError('Bad encoding value: ' + str(encoding))
        if local not in (True, False):
            raise ValueError('Bad local value: ' + str(local))
        if priority and not streaming:
            raise ValueError('Priority requires streaming mode')
        if encoding == ClientProperties.ENCODING_ZLIB and not streaming:
            raise ValueError('Zlib encoding requires streaming mode')
        if encoding == ClientProperties.ENCODING_GZIP and streaming:
            raise ValueError('Gzip encoding forbids streaming mode')
        if (local and
            (not streaming
             or serialization != ztreamy.SERIALIZATION_NONE
             or encoding != ClientProperties.ENCODING_PLAIN
             or priority)):
            raise ValueError('Incompatible properties for a local client')
        key = (streaming, serialization, encoding, local, priority)
        if key in ClientPropertiesFactory.instances:
            return ClientPropertiesFactory.instances[key]
        else:
            instance = ClientProperties(*key)
            ClientPropertiesFactory.instances[key] = instance
            return instance

    @staticmethod
    def create_local_client():
        return ClientPropertiesFactory.create( \
                        streaming=True,
                        serialization=ztreamy.SERIALIZATION_NONE,
                        local=True)


class EventsPack(object):
    """A pack of events with a serialization cache.

    The intention is avoiding serializing several times the same events.

    """
    def __init__(self, events):
        self.events = events
        self.serialized_cache = {}

    def serialize(self, serialization):
        if not serialization in self.serialized_cache:
            serialized = ztreamy.serialize_events(self.events,
                                                  serialization=serialization)
            self.serialized_cache[serialization] = serialized
        else:
            serialized = self.serialized_cache[serialization]
        return serialized

    def __len__(self):
        return len(self.events)


class Dispatcher(object):
    def __init__(self, stream, properties):
        self.stream = stream
        self.properties = properties
        self.subscriptions = []

    def subscribe(self, client):
        self.subscriptions.append(client)
        logging.info('Client subscribed: {} {}'.format(self.stream.path,
                                                       client.properties))


    def unsubscribe(self, client):
        self.subscriptions.remove(client)
        logging.info('Client unsubscribed: {} {}'.format(self.stream.path,
                                                         client.properties))

    def num_subscriptions(self):
        return len(self.subscriptions)

    def close(self):
        for client in self.subscriptions:
            client.close()
        self.subscriptions = []

    def __len__(self):
        return self.num_subscriptions()

    def periodic_maintenance(self):
        pass


class LocalDispatcher(Dispatcher):
    def __init__(self, stream, properties):
        if not properties.local:
            raise ValueError('Incompatible properties for LocalDispacther')
        super(LocalDispatcher, self).__init__(stream, properties)

    def dispatch(self, events_pack):
        if len(self.subscriptions):
            logging.debug('{} {}: {}'.format(self.stream.path,
                                             self.properties,
                                             len(self.subscriptions)))
        if len(self.subscriptions) and len(events_pack):
            for client in self.subscriptions:
                client.send_events(events_pack.events)


class SimpleDispatcher(Dispatcher):
    def __init__(self, stream, properties):
        if (properties.encoding != ClientProperties.ENCODING_PLAIN
            and properties.encoding != ClientProperties.ENCODING_GZIP):
            raise ValueError('PlainDispatcher requires PLAIN or GZIP encoding')
        if (properties.encoding == ClientProperties.ENCODING_GZIP
            and properties.streaming):
            raise ValueError('Serialization is incompatible '
                             'with GZIP encoding')
        super(SimpleDispatcher, self).__init__(stream, properties)
        self.last_event_time = time.time()

    def subscribe(self, client):
        if not len(self.subscriptions):
            self.last_event_time = time.time()
        super(SimpleDispatcher, self).subscribe(client)

    def dispatch(self, events_pack):
        if len(self.subscriptions):
            logging.debug('{} {}: {}'.format(self.stream.path,
                                             self.properties,
                                             len(self.subscriptions)))
        if len(self.subscriptions) and len(events_pack):
            self.last_event_time = time.time()
            data = events_pack.serialize(self.properties.serialization)
            if self.properties.encoding == ClientProperties.ENCODING_GZIP:
                data = compress_gzip(data)
            for client in self.subscriptions:
                client.send(data)

    def periodic_maintenance(self):
        if len(self.subscriptions):
            current_time = time.time()
            if not self.properties.streaming:
                for client in self.subscriptions:
                    if current_time - client.creation_time > 43200:
                        client.close()
                        self.unsubscribe(client)
            elif time.time() - self.last_event_time > 595:
                logging.debug('{} {}: sending keep-alive event'.format( \
                                                self.stream.path,
                                                self.properties))
                keep_alive = events.Command('', 'ztreamy-command',
                                            'Test-Connection')
                self.dispatch(EventsPack([keep_alive]))


class ZlibDispatcher(Dispatcher):
    def __init__(self, stream, properties):
        if (properties.encoding != ClientProperties.ENCODING_ZLIB
            or not properties.streaming):
            raise ValueError('MainZlibDispatcher requires ZLIB encoding ',
                             'and streaming mode.')
        super(ZlibDispatcher, self).__init__(stream, properties)
        self.groups = [SubscriptionGroup(properties)]
        self.new_subscriptions = []
        self.last_event_time = time.time()

    def subscribe(self, client):
        if not len(self.subscriptions):
            self.last_event_time = time.time()
        super(ZlibDispatcher, self).subscribe(client)
        self.new_subscriptions.append(client)

    def unsubscribe(self, client):
        super(ZlibDispatcher, self).unsubscribe(client)
        for group in self.groups:
            if group.unsubscribe(client):
                break
        else:
            self.new_subscriptions.remove(client)

    def dispatch(self, events_pack):
        self._group_maintenance()
        if self.new_subscriptions:
            if self.groups[-1].is_reset:
                self.groups[-1].merge_clients(self.new_subscriptions)
            else:
                new_group = SubscriptionGroup(self.properties)
                new_group.merge_clients(self.new_subscriptions)
                self.groups.append(new_group)
            self.new_subscriptions = []
        if len(self.subscriptions):
            logging.debug('{} {}: {} ({} groups)'.format( \
                                                self.stream.path,
                                                self.properties,
                                                len(self.subscriptions),
                                                len(self.groups)))
            if len(self.groups) > 1:
                for i, group in enumerate(self.groups):
                    logging.debug('    #{}: {} | {}'.format(i, len(group),
                                                           group.data_counter))
            if len(events_pack):
                self.last_event_time = time.time()
                for group in self.groups:
                    group.dispatch(events_pack)

    def close(self):
        for group in self.groups:
            for client in group:
                client.close()
        self.groups = [SubscriptionGroup(self.properties)]

    def periodic_maintenance(self):
        if (len(self.subscriptions)
            and time.time() - self.last_event_time > 595):
            logging.debug('{} {}: sending keep-alive event'.format( \
                                                        self.stream.path,
                                                        self.properties))
            keep_alive = events.Command('', 'ztreamy-command',
                                        'Test-Connection')
            self.dispatch(EventsPack([keep_alive]))

    def _group_maintenance(self):
        if len(self.groups) > 1 and self.groups[0].is_empty():
            del(self.groups[0])
        if len(self.groups) > 1:
            for group in self.groups[1:]:
                if group.is_empty():
                    group.terminated = True
                elif group.data_counter >= 32768:
                    # The state of this group should be the same
                    # as for the main compressor.
                    self.groups[0].merge_group(group, full_flush=False)
            self.groups = [g for g in self.groups if not g.terminated]


    def _merge_all_groups(self):
        for group in self.groups[1:]:
            self.groups[0].merge_group(group)
        self.groups = [self.groups[0]]


class SubscriptionGroup(object):
    def __init__(self, properties, compression_level=6):
        self.properties = properties
        self.subscriptions = []
        if properties.encoding == ClientProperties.ENCODING_ZLIB:
            self.compressor = zlib.compressobj(compression_level)
            self.initial_data = self.compressor.compress('')
        else:
            self.compressor = None
            self.initial_data = None
        self.is_reset = True
        self.data_counter = 0
        self.terminated = False
        self.inside_dispatch = False
        self.to_unsubscribe = []

    def subscribe(self, client):
        if not self.is_reset:
            raise ValueError('Compressor in dirty state')
        self.subscriptions.append(client)
        if self.compressor and client.is_fresh:
            client.send(self.initial_data, flush=False)

    def subscribe_clients(self, clients):
        if not self.is_reset:
            raise ValueError('Compressor in dirty state')
        self.subscriptions.extend(clients)
        if self.compressor:
            for client in clients:
                if client.is_fresh:
                    client.send(self.initial_data, flush=False)

    def unsubscribe(self, client):
        if not self.inside_dispatch:
            try:
                self.subscriptions.remove(client)
            except ValueError:
                return False
            else:
                return True
        else:
            if client in self.subscriptions:
                self.to_unsubscribe.append(client)
                return True
            else:
                return False

    def is_empty(self):
        return not self.subscriptions

    def merge_group(self, group, full_flush=True):
        if not group.is_empty():
            if not self.is_reset and full_flush:
                self._full_flush()
            self.subscriptions.extend(group.subscriptions)
        group.terminated = True

    def merge_clients(self, clients):
        if clients:
            if not self.is_reset:
                self._full_flush()
            self.subscribe_clients(clients)

    def dispatch(self, events_pack):
        if len(self.subscriptions):
            serialized = events_pack.serialize(self.properties.serialization)
            if self.compressor is not None:
                data = self.compressor.compress(serialized)
                data += self.compressor.flush(zlib.Z_SYNC_FLUSH)
            else:
                data = serialized
            self.inside_dispatch = True
            for client in self.subscriptions:
                client.send(data)
            self.inside_dispatch = False
            for client in self.to_unsubscribe:
                self.unsubscribe(client)
            self.is_reset = False
            self.data_counter += len(serialized)

    def __len__(self):
        return len(self.subscriptions)

    def __iter__(self):
        return iter(self.subscriptions)

    def _full_flush(self):
        if self.compressor is not None:
            self.compressor.flush(zlib.Z_FULL_FLUSH)


def compress_gzip(data):
    sio = StringIO.StringIO()
    with gzip.GzipFile(fileobj=sio, mode='wb') as f:
        f.write(data)
    return sio.getvalue()

def compress_zlib(data):
    compressor = zlib.compressobj()
    data = compressor.compress(data)
    data += compressor.flush(zlib.Z_SYNC_FLUSH)
    return data
