import tornado.ioloop
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.curl_httpclient import CurlAsyncHTTPClient
import tornado.options
import logging
import zlib
import sys

import streamsem
from streamsem import events
import streamsem.rdfevents
from streamsem import logger

transferred_bytes = 0
data_count = 0

param_max_clients = 32768

#AsyncHTTPClient.configure("tornado.simple_httpclient.SimpleAsyncHTTPClient")
AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

class Client(object):
    def __init__(self, source_urls, event_callback, error_callback=None,
                 ioloop=None, parse_event_body=True, separate_events=True):
        self.source_urls = source_urls
        self.clients = \
            [AsyncStreamingClient(url, event_callback=event_callback,
                         error_callback=error_callback,
                         connection_close_callback=self._client_close_callback,
                         parse_event_body=parse_event_body,
                         separate_events=separate_events) \
                 for url in source_urls]
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self._closed = False
        self._looping = False
        self.active_clients = []

    def start(self, loop=True):
        for client in self.clients:
            client.start(False)
            self.active_clients.append(client)
        if loop:
            self._looping = True
            self.ioloop.start()
            self._looping = False

    def stop(self):
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


class AsyncStreamingClient(object):
    def __init__(self, url, event_callback=None, error_callback=None,
                 connection_close_callback=None,
                 ioloop=None, parse_event_body=True, separate_events=True):
        self.url = url
        self.event_callback = event_callback
        self.error_callback = error_callback
        self.connection_close_callback = connection_close_callback
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.parse_event_body = parse_event_body
        self.separate_events = separate_events
        self._closed = False
        self._looping = False
        self._compressed = False
        self._deserializer = events.Deserializer()
#        self.data_history = []

    def start(self, loop=False):
        self.http_client = AsyncHTTPClient(max_clients=param_max_clients)
        req = HTTPRequest(self.url, streaming_callback=self._stream_callback,
                          request_timeout=0, connect_timeout=0)
        self.http_client.fetch(req, self._request_callback)
        if loop:
            self._looping = True
            self.ioloop.start()
            self._looping = False

    def stop(self):
        """Stops the HTTP client and, if looping, the ioloop instance.

        Note: if the backend behind
        tornado.httpclient.AsyncHTTPClient() is SimpleHTTPClient,
        invoking stop() does not actually close the HTTP connections
        (as of Tornado branch master september 1st 2011).

        """
        if not self._closed:
            self.http_client.close()
            self._closed = True
            if self._looping:
                self.ioloop.stop()
                self._looping = False

    def _stream_callback(self, data):
        global transferred_bytes
        transferred_bytes += len(data)
        evs = self._deserialize(data, parse_body=self.parse_event_body)
        for e in evs:
            logger.logger.event_delivered(e)
        if self.event_callback is not None:
            if not self.separate_events:
                self.event_callback(evs)
            else:
                for ev in evs:
                    self.event_callback(ev)

    def _request_callback(self, response):
        if response.error:
            if self.error_callback is not None:
                self.error_callback('Error in HTTP request',
                                    http_error=response.error)
        elif len(response.body) > 0:
#            self.data_history.append(response.body)
            self._notify_event(response.body)
        logging.info('Connection closed by server')
        if self._looping:
            self.ioloop.stop()
            self._looping = False
        if self.connection_close_callback:
            self.connection_close_callback(self)

    def _reset_compression(self):
        self._compressed = True
        self._decompressor = zlib.decompressobj()

    def _deserialize(self, data, parse_body=True):
        evs = []
        event = None
        compressed_len = len(data)
        if self._compressed:
            data = self._decompressor.decompress(data)
        logger.logger.data_received(compressed_len, len(data))
        self._deserializer.append_data(data)
        event = self._deserializer.deserialize_next(parse_body=parse_body)
        while event is not None:
            if isinstance(event, events.Command):
                if event.command == 'Set-Compression':
                    self._reset_compression()
                    pos = self._deserializer.data_consumed()
                    self._deserializer.reset()
                    evs.extend(self._deserialize(data[pos:], parse_body))
                    return evs
            else:
                evs.append(event)
            event = self._deserializer.deserialize_next(parse_body=parse_body)
        return evs


class EventPublisher(object):
    """Sends events to a server in order to publish them.

    Uses an asynchronous HTTP client, but does not manage an ioloop
    itself. The ioloop must be run by the calling code.

    """
    def __init__(self, server_url, io_loop=None):
        self.server_url = server_url
        self.http_client = CurlAsyncHTTPClient(io_loop=io_loop)
        self.headers = {'Content-Type': streamsem.mimetype_event}

    def publish(self, event, callback=None):
        logger.logger.event_published(event)
        body = str(event)
        req = HTTPRequest(self.server_url, body=body, method='POST',
                          headers=self.headers, request_timeout=0,
                          connect_timeout=0)
        callback = callback or self._request_callback
        self.http_client.fetch(req, callback)

    def close(self):
        self.http_client.close()
        self.http_client=None

    def _request_callback(self, response):
        if response.error:
            logging.error(response.error)
        else:
            logging.info('Event successfully sent to server')


def read_cmd_options():
    from optparse import OptionParser, Values
    tornado.options.define('eventlog', default=False,
                           help='dump event log',
                           type=bool)
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 1:
        options.stream_urls = remaining
    else:
        OptionParser().error('At least one source stream URL required')
    return options

def main():
    import time
    def handle_event(event):
        sys.stdout.write(str(event))
    def handle_error(message, http_error=None):
        if http_error is not None:
            logging.error(message + ': ' + str(http_error))
        else:
            logging.error(message)
    def stop_client():
        client.stop()
    options = read_cmd_options()
#    import streamsem.filters
#    filter = streamsem.filters.SimpleTripleFilter(handle_event,
#                                        predicate='http://example.com/temp')
    client = Client(options.stream_urls,
                    event_callback=handle_event,
#                    event_callback=filter.filter_event,
                    error_callback=handle_error)
#    tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 6, stop_client)
    node_id = streamsem.random_id()
    if tornado.options.options.eventlog:
        logger.logger = logger.StreamsemLogger(node_id,
                                               'client-' + node_id + '.log')
    try:
        client.start(loop=True)
    except KeyboardInterrupt:
        pass
    finally:
        logger.logger.close()

if __name__ == "__main__":
    main()
