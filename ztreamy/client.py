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
""" Clients that communicate with stream servers to send or receive events.

There are several clients that receive events: 'Client',
'AsyncStreamingClient' and 'SynchronousClient'. Both 'Client' and
'AsyncStreamingClient' are asynchronous. Their difference is that
'Client' can listen to several event streams at the same time. It is
implemented as a wrapper on top of 'AsyncStreamingClient'. On the
other hand, 'SynchronousClient' implements a synchronous client for
just one stream.

'EventPublisher' is an asynchronous class that sends events to be
served in a stream. 'SynchronousEventPublisher' has a similar
interface, but is synchronous.

"""
import logging
import sys
import urllib2
import httplib
import datetime
import random
import os
import os.path
import base64

import tornado.ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.curl_httpclient import CurlAsyncHTTPClient
import tornado.simple_httpclient
import tornado.options
import tornado.gen

import ztreamy
from ztreamy import Deserializer, Command, Filter
from ztreamy import split_url

transferred_bytes = 0
data_count = 0

#AsyncHTTPClient.configure("tornado.simple_httpclient.SimpleAsyncHTTPClient")
AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

def configure_max_clients(max_clients):
    """Sets the maximum number of simultaneous clients.

    Tornado's AsyncHTTPClient implementations may impose a
    configurable maximum number of simultaneous fecth() operations
    that can be performed on the same IOLoop. This number limits
    the number of simultaneous streaming and long-polling clients.

    The current default for this value in CurlAsyncHTTPClient is 10.
    Applications that need clients for more than 10 simultaneous
    streams must configure a bigger limit by calling this function.

    Be aware that this number may have significant consequences
    in the amount of RAM memory used by Tornado, even when there
    are no active clients. Don't set a value much higher than the
    actual number of simultaneous streaming clients you need.

    """
    AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient",
                              max_clients=max_clients)


class ReconnectionManager(object):
    def __init__(self):
        self.num_attempts = 0

    def notify_success(self):
        self.num_attempts = 0

    def notify_failure(self):
        self.num_attempts += 1

    def compute_delay(self):
        return random.uniform(0.001, 0.2 * 2 ** min(self.num_attempts, 10))


class Client(object):
    """Asynchronous client for multiple stream sources.

    This client is able to receive events from multiple streams, which
    may be remote (trhough HTTP) or local to the process. Its internal
    implementation is based on the 'AsyncStreamingClient' and
    'LocalClient' classes.

    """
    def __init__(self, streams, event_callback, error_callback=None,
                 connection_close_callback=None,
                 source_start_callback=None, source_finish_callback=None,
                 label=None, retrieve_missing_events=False,
                 ioloop=None, parse_event_body=True, separate_events=True,
                 disable_compression=False):
        """Creates a new client for one or more stream URLs.

        'streams' is a list of streams to connect to. Each stream can
        be either a string representing the stream URL or a local
        'server.Stream' (or compatible) object. In the later case, the
        connection is just local to the process, without using the
        network stack. Instead of a list, a single URL or a single
        stream object are also accepted.

        For every single received event, the 'event_callback' function
        is invoked. It receives an event object as parameter.

        If 'separate_events' is set to None, then the event callback
        will receive a list of events instead of a single events.

        A 'label' (string) may be set to this client. Setting a label
        allows the client to save the id of the latest event it received
        and ask for missed events when the client is run again.
        Set 'retrieve_missing_events' to True in order to do that.
        If 'retrieve_missing_events' is True, a non-empty label must be set.

        If a 'ioloop' object is given, the client will block on it
        apon calling the 'start()' method. If not, it will block on
        the default 'ioloop' of Tornado.

        """
        if not isinstance(streams, list):
            streams = [streams]
        self.clients = []
        for stream in streams:
            if isinstance(stream, basestring):
                self.clients.append(AsyncStreamingClient(stream,
                         event_callback=event_callback,
                         error_callback=error_callback,
                         source_start_callback=source_start_callback,
                         source_finish_callback=source_finish_callback,
                         connection_close_callback=self._client_close_callback,
                         label=label,
                         retrieve_missing_events=retrieve_missing_events,
                         parse_event_body=parse_event_body,
                         separate_events=separate_events,
                         disable_compression=disable_compression))
            else:
                self.clients.append(LocalClient(stream,
                                            event_callback=event_callback,
                                            separate_events=separate_events))
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self._closed = False
        self._looping = False
        self.active_clients = []
        self.connection_close_callback = connection_close_callback

    def start(self, loop=False):
        """Starts the client.

        This function has to be called in order to connect to the
        streams and begin to receive events.

        If 'loop' is true (which is the default), the server will
        block on the ioloop until 'close()' is called.

        """
        if self._closed:
            raise Exception('This client has already been closed.')
        for client in self.clients:
            client.start(False)
            self.active_clients.append(client)
        if loop:
            self._looping = True
            self.ioloop.start()
            self._looping = False

    def stop(self):
        """Stops and closes this client.

        The client can no longer be used in the future.

        If the server is blocked on the ioloop in the 'start()'
        method, it is released.

        """
        if not self._closed:
            for client in self.clients:
                client.stop()
        self.active_clients = []
        self._closed = True
        if self._looping:
            self.ioloop.stop()
            self._looping = False

    def _client_close_callback(self, client):
        if client in self.active_clients:
            self.active_clients.remove(client)
            if len(self.active_clients) == 0 and self._looping:
                self.ioloop.stop()
                self._looping = False
                if self.connection_close_callback:
                    self.connection_close_callback()


class LocalClient(object):
    """Client for local use in the same process as the stream.

    This client does not use the network to receive the events from a
    stream running in the same process.

    The normal usage of this class is:

    client = LocalClient(stream, callback)
    client.start()

    In order to disconnect from the stream:

    client.stop()

    """
    def __init__(self, stream, event_callback, separate_events=True):
        """Creates a new local client, but does not start it.

        'stream' is a 'server.Stream' object (or an object of a
        compatible class). 'event_callback' is the callback function
        which will receive the events. The events will be received as
        single event objects (the default, when 'separate_events' is
        True) or as a list of event objects (when 'separate_events' is
        set to False).

        The connection to the stream is not established until
        'start()' is invoked.

        """
        if isinstance(event_callback, Filter):
            event_callback = event_callback.filter_event
        self.stream = stream
        self.event_callback = event_callback
        self.separate_events = separate_events

    def start(self, loop=False):
        """Starts listening to the stream.

        'loop' must always be False, but is mainatined for
        compatibility with other clients.

        """
        assert loop is False
        self.client_handle = \
             self.stream.create_local_client(self.event_callback,
                                          separate_events=self.separate_events)

    def stop(self):
        """Stops listening to the stream."""
        self.client_handle.close()


class AsyncStreamingClient(object):
    """Asynchronous client for a single event source.

    If you need to receive events from several sources, use the class
    'Client' instead.

    """
    def __init__(self, url, event_callback=None, error_callback=None,
                 connection_close_callback=None,
                 source_start_callback=None, source_finish_callback=None,
                 label=None, retrieve_missing_events=False,
                 ioloop=None, parse_event_body=True, separate_events=True,
                 reconnect=True, disable_compression=False):
        """Creates a new client for a given stream URL.

        The client connects to the stream URL given by 'url'.  For
        every single received event, the 'event_callback' function is
        invoked. It receives an event object as parameter.

        If 'separate_events' is set to None, then the event callback
        will receive a list of events instead of a single events.

        A 'label' (string) may be set to this client. Setting a label
        allows the client to save the id of the latest event it received
        and ask for missed events when the client is run again.
        Set 'retrieve_missing_events' to True in order to do that.
        If 'retrieve_missing_events' is True, a non-empty label must be set.

        If a 'ioloop' object is given, the client will block on it
        apon calling the 'start()' method. If not, it will block on
        the default 'ioloop' of Tornado.

        When 'reconnect' is True (which is the default), the client tries
        to automatically reconnect when it loses connection
        with the server, following an exponential back-off mechanism.

        """
        if retrieve_missing_events and not label:
            raise ValueError('Retrieving missing events'
                             ' requires a client label')
        if isinstance(event_callback, Filter):
            event_callback = event_callback.filter_event
        self.url = url
        self.event_callback = event_callback
        self.error_callback = error_callback
        self.connection_close_callback = connection_close_callback
        self.source_start_callback = source_start_callback
        self.source_finish_callback = source_finish_callback
        self.label = label
        if retrieve_missing_events:
            self.status_file = self._create_status_file(False)
        elif label:
            self.status_file = self._create_status_file(True)
        else:
            self.status_file = None
        self.last_event_id = None
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.parse_event_body = parse_event_body
        self.separate_events = separate_events
        self._closed = False
        self._looping = False
        self._deserializer = Deserializer()
        self.reconnect = reconnect
        self.disable_compression = disable_compression
        self.reconnection = ReconnectionManager()
#        self.data_history = []

    def start(self, loop=False):
        """Starts the client.

        This function has to be called in order to connect to the
        streams and begin to receive events.

        If 'loop' is True (the default is False), the server will
        block on the ioloop until 'close()' is called.

        """
        self._connect()
        if loop:
            self._looping = True
            self.ioloop.start()
            self._looping = False

    def stop(self, notify_connection_close=True):
        """Stops and closes this client.

        The client can no longer be used in the future.

        Unless the keywork parameter `notify_connection_close` is set
        to false, the connection close callback will be invoked if set
        for this client.

        If the server is blocked on the ioloop in the 'start()'
        method, it is released.

        Note: if the backend behind
        'tornado.httpclient.AsyncHTTPClient()' is 'SimpleHTTPClient',
        invoking 'stop()' does not actually close the HTTP connections
        (as of Tornado branch master september 1st 2011).

        """
        self._finish_internal(notify_connection_close)

    def _connect(self):
        http_client = AsyncHTTPClient()
        last_event_received = self._read_last_event_id()
        if last_event_received is None:
            url = self.url
        else:
            url = self.url + '?last-seen=' + last_event_received
        if not self.disable_compression:
            headers = {'Accept-Encoding': 'deflate;q=1, identity;q=0.5'}
        else:
            headers = {'Accept-Encoding': 'identity'}
        req = HTTPRequest(url, streaming_callback=self._stream_callback,
                          headers=headers,
                          request_timeout=0, connect_timeout=0)
        http_client.fetch(req, self._request_callback)
        self.reconnection.notify_failure()
        logging.info('Connecting to {}'.format(self.url))

    def _reconnect(self):
        t = self.reconnection.compute_delay()
        logging.info('Disconnected from {}. Next attempt in {:.02f}s'\
                     .format(self.url, t))
        self.ioloop.add_timeout(datetime.timedelta(seconds=t), self._connect)

    def _finish_internal(self, notify_connection_close):
        if self._closed:
            return
        if (notify_connection_close
            and self.connection_close_callback is not None):
            self.connection_close_callback(self)
        if self._looping:
            self.ioloop.stop()
            self._looping = False
        self._closed = True

    def _stream_callback(self, data):
        self.reconnection.notify_success()
        self._process_received_data(data)

    def _request_callback(self, response):
        if response.error:
            if (not response.error.code // 100 == 4
                and not self._closed
                and self.reconnect):
                self._reconnect()
                finish = False
            else:
                if self.error_callback is not None:
                    self.error_callback('Error in HTTP request',
                                        http_error=response.error)
                finish = True
        else:
            if len(response.body) > 0:
                self._process_received_data(response.body)
            self._reconnect()
            finish = False
        if finish:
            logging.info('Finishing client')
            self._finish_internal(True)

    def _process_received_data(self, data):
        global transferred_bytes
        transferred_bytes += len(data)
        evs = self._deserialize(data, parse_body=self.parse_event_body)
        if self.event_callback is not None:
            if not self.separate_events:
                self.event_callback(evs)
            else:
                for ev in evs:
                    self.event_callback(ev)
        if len(evs) > 0:
            self._write_last_event_id(evs[-1])

    def _deserialize(self, data, parse_body=True):
        evs = []
        event = None
        self._deserializer.append_data(data)
        event = self._deserializer.deserialize_next(parse_body=parse_body)
        while event is not None:
            if isinstance(event, Command):
                if event.command == 'Event-Source-Started':
                    if self.source_start_callback:
                        self.source_start_callback()
                    evs.append(event)
                elif event.command == 'Event-Source-Finished':
                    if self.source_finish_callback:
                        self.source_finish_callback()
                    evs.append(event)
                elif event.command == 'Stream-Finished':
                    self._finish_internal(True)
                    ## logging.info('Stream finished')
            else:
                evs.append(event)
            event = self._deserializer.deserialize_next(parse_body=parse_body)
        return evs

    def _create_status_file(self, overwrite):
        dirname = '.ztreamy-client-' + self.label
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        status_file = os.path.join(dirname, base64.urlsafe_b64encode(self.url))
        if overwrite:
            mode = 'w'
        else:
            mode = 'a'
        with open(status_file, mode=mode):
            pass
        return status_file

    def _write_last_event_id(self, event):
        if self.status_file is not None:
            with open(self.status_file, mode='w') as f:
                f.write(event.event_id)
        else:
            self.last_event_id = event.event_id

    def _read_last_event_id(self):
        if self.status_file is not None:
            with open(self.status_file) as f:
                event_id_read = f.read().strip()
            if event_id_read:
                event_id = event_id_read
            else:
                event_id = None
        else:
            event_id = self.last_event_id
        return event_id


class SynchronousClient(object):
    """Synchronous client.

    This client should be used in long-polling mode.

    """
    def __init__(self, server_url, parse_event_body=True,
                 last_event_seen=None):
        self.server_url = server_url
        self.last_event_seen = last_event_seen
        self.deserializer = Deserializer()
        self.parse_event_body = parse_event_body
        self.stream_finished = False

    def receive_events(self):
        url = self.server_url
        if self.last_event_seen is not None:
            url += '?last-seen=' + self.last_event_seen
        connection = urllib2.urlopen(url)
        data = connection.read()
        evs = self.deserializer.deserialize(data, complete=True,
                                            parse_body=self.parse_event_body)
        connection.close()
        if len(evs) > 0:
            self.last_event_seen = evs[-1].event_id
        for event in evs:
            if (isinstance(event, Command)
                and event.command == 'Stream-Finished'):
                self.stream_finished = True
                break
        return [e for e in evs if not isinstance(e, Command)]


class EventPublisher(object):
    """Publishes events by sending them to a server. Asynchronous.

    Uses an asynchronous HTTP client, but does not manage an ioloop
    itself. The ioloop must be run by the calling code.

    """

    _headers = {'Content-Type': ztreamy.event_media_type}

    def __init__(self, server_url, io_loop=None,
                 serialization_type=ztreamy.SERIALIZATION_ZTREAMY):
        """Creates a new 'EventPublisher' object.

        Events are sent in separate HTTP requests to the server given
        by 'server_url'.

        """
        if server_url.endswith('/publish'):
            self.server_url = server_url
        elif server_url.endswith('/'):
            self.server_url = server_url + 'publish'
        else:
            self.server_url = server_url + '/publish'
        self.ioloop = io_loop or tornado.ioloop.IOLoop.instance()
        self.http_client = CurlAsyncHTTPClient(self.ioloop)
        self.serialization_type = serialization_type
        self.headers = dict(EventPublisher._headers)
        if serialization_type == ztreamy.SERIALIZATION_JSON:
            self.headers['Content-Type'] = ztreamy.json_media_type

    def publish(self, event, callback=None):
        """Publishes a new event.

        The event is sent to the server in a new HTTP request. If a
        'callback' is given, it will be called when the response is
        received from the server. The callback receives a
        tornado.httpclient.HTTPResponse parameter.

        """
        self.publish_events([event], callback=callback)

    def publish_events(self, events, callback=None):
        """Publishes a list of events.

        The events in the list 'events' are sent to the server in a
        new HTTP request. If a 'callback' is given, it will be called
        when the response is received from the server. The callback
        receives a tornado.httpclient.HTTPResponse parameter.

        """
        body = ztreamy.serialize_events(events,
                                        serialization=self.serialization_type)
        self._send_request(body, callback=callback)

    def close(self):
        """Closes the event publisher.

        This object should not be used anymore.

        """
        ## self.http_client.close()
        self.http_client=None

    def _request_callback(self, response):
        if response.error:
            logging.error(response.error)
        ## else:
        ##     logging.info('Event successfully sent to server')

    def _send_request(self, body, callback=None):
        req = HTTPRequest(self.server_url, body=body, method='POST',
                          headers=self.headers, request_timeout=0,
                          connect_timeout=0)
        callback = callback or self._request_callback
        # Enqueue a new callback in the ioloop, to avoid problems
        # when this code is run from a callback of the HTTP client
        def fetch():
            self.http_client.fetch(req, callback)
        self.ioloop.add_callback(fetch)


class SynchronousEventPublisher(object):
    """Publishes events by sending them to a server. Synchronous.

    Uses a synchronous HTTP client.

    """
    _headers = {'Content-Type': ztreamy.event_media_type}

    def __init__(self, server_url,
                 serialization_type=ztreamy.SERIALIZATION_ZTREAMY):
        """Creates a new 'SynchronousEventPublisher' object.

        Events are sent in separate HTTP requests to the server given
        by 'server_url'.

        """
        scheme, self.hostname, self.port, self.path = split_url(server_url)
        assert scheme == 'http'
        if not self.path.endswith('/publish'):
            if self.path.endswith('/'):
                self.path = self.path + 'publish'
            else:
                self.path = self.path + '/publish'
        self.serialization_type = serialization_type
        self.headers = dict(SynchronousEventPublisher._headers)
        if serialization_type == ztreamy.SERIALIZATION_JSON:
            self.headers['Content-Type'] = ztreamy.json_media_type

    def publish(self, event):
        """Publishes a new event.

        The event is sent to the server in a new HTTP request. Returns
        True if the data is received correctly by the server.

        """
        return self.publish_events([event])

    def publish_events(self, events, return_response=False):
        """Publishes a list of events.

        The events in the list 'events' are sent to the server in a new
        HTTP request.

        """
        body = ztreamy.serialize_events(events,
                                        serialization=self.serialization_type)
        conn = httplib.HTTPConnection(self.hostname, self.port)
        conn.request('POST', self.path, body, self.headers)
        response = conn.getresponse()
        if response.status != 200:
            logging.error(str(response.status) + ' ' + response.reason)
        if return_response:
            return response
        else:
            return response.status == 200

    def close(self):
        """Closes the event publisher.

        It does nothing in this class, but is maintained for
        compatibility with the asynchronous publisher.

        """
        pass


class ContinuousEventPublisher(object):
    """Continuously publish events through a single long-lived HTTP request.

    """
    def __init__(self, server_url, io_loop=None,
                 serialization_type=ztreamy.SERIALIZATION_ZTREAMY,
                 buffering_time=1.0):
        if server_url.endswith('/publish'):
            self.server_url = server_url + '-cont'
        elif server_url.endswith('/'):
            self.server_url = server_url + 'publish-cont'
        else:
            self.server_url = server_url + '/publish-cont'
        self.io_loop = io_loop or tornado.ioloop.IOLoop.instance()
        # The CURL client does not allow the `body_producer` style.
        # Therefore we use a `SimpleAsyncHTTPClient`:
        self.http_client = \
              tornado.simple_httpclient.SimpleAsyncHTTPClient(self.io_loop)
        self.headers = {}
        if serialization_type == ztreamy.SERIALIZATION_ZTREAMY:
            self.serialization_type = ztreamy.SERIALIZATION_ZTREAMY
            self.headers['Content-Type'] = ztreamy.stream_media_type
        elif serialization_type == ztreamy.SERIALIZATION_LDJSON:
            self.serialization_type = ztreamy.SERIALIZATION_LDJSON
            self.headers['Content-Type'] = ztreamy.ldjson_media_type
        else:
            raise ValueError('Bad serialization type')
        self.buffering_time = buffering_time
        self.next_publication = None
        self.running = False
        self.pending_events = []
        self.reconnection = ReconnectionManager()

    @tornado.gen.coroutine
    def start(self):
        self.running = True
        while self.running:
            self.next_publication = self.io_loop.time()
            req = HTTPRequest(self.server_url,
                              method='POST',
                              body_producer=self._body_producer,
                              headers=self.headers,
                              request_timeout=0,
                              connect_timeout=5.0)
            logging.debug('Continuous HTTP request {}'.format(self.server_url))
            try:
                self.response = yield self.http_client.fetch(req)
            except Exception as e:
                logging.warning('Continuous request: {}'.format(e))
            logging.debug('Continuous request finished, pending {} events'\
                          .format(len(self.pending_events)))
            self.reconnection.notify_failure()
            if self.running:
                delay = self.reconnection.compute_delay()
                logging.warning('Retrying in {}s'.format(delay))
                yield tornado.gen.sleep(delay)

    def stop(self):
        self.running = False

    def publish(self, event):
        logging.debug('Publishing 1 event')
        self.pending_events.append(event)

    def publish_events(self, events):
        logging.debug('Publishing {} events'.format(len(events)))
        self.pending_events.extend(events)

    @tornado.gen.coroutine
    def _body_producer(self, write):
        while True:
            if self.pending_events:
                data = ztreamy.serialize_events(self.pending_events,
                                         serialization=self.serialization_type)
                yield write(data)
                self.reconnection.notify_success()
                self.pending_events = []
            if self.running:
                self.next_publication += self.buffering_time
                delay = self.next_publication - self.io_loop.time()
                if delay <= 0:
                    self.next_publication = (self.io_loop.time()
                                             + self.buffering_time)
                    delay = self.buffering_time
                yield tornado.gen.sleep(delay)
            else:
                break


class LocalEventPublisher(object):
    def __init__(self, stream, ioloop=None):
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.stream = stream

    def publish(self, event):
        """Publishes a new event.

        Returns True.

        """
        self.stream.dispatch_event(event)

    def publish_events(self, events):
        """Publishes a list of events.

        Returns True.

        """
        self.stream.dispatch_events(events)

    def close(self):
        """Closes the event publisher.

        It does nothing in this class, but is maintained for
        compatibility with the asynchronous publisher.

        """
        pass


def read_cmd_options():
    from optparse import OptionParser, Values
    tornado.options.define('label', default=None,
                           help='define a client label',
                           type=str)
    tornado.options.define('missing', default=False,
                           help=('retrieve missing events '
                                 '(requires a client label)'),
                           type=bool)
    tornado.options.define('deflate', default=True,
                           help='Accept compressed data with deflate',
                           type=bool)
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 1:
        options.stream_urls = remaining
    else:
        OptionParser().error('At least one source stream URL required')
    return options

def main():
    def handle_event(event):
        sys.stdout.write(str(event))
        sys.stdout.flush()
    def handle_error(message, http_error=None):
        if http_error is not None:
            logging.error(message + ': ' + str(http_error))
        else:
            logging.error(message)
    def stop_client():
        client.stop()
    options = read_cmd_options()
#    import ztreamy.filters
#    filter = ztreamy.filters.SimpleTripleFilter(handle_event,
#                                        predicate='http://example.com/temp')
    disable_compression = not tornado.options.options.deflate
    retrieve_missing_events = tornado.options.options.missing
    client_label = tornado.options.options.label
    client = Client(options.stream_urls,
                    event_callback=handle_event,
#                    event_callback=filter.filter_event,
                    error_callback=handle_error,
                    disable_compression=disable_compression,
                    label=client_label,
                    retrieve_missing_events=retrieve_missing_events)
#    import time
#    tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 6, stop_client)
    try:
        client.start(loop=True)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
