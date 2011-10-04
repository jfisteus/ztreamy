import tornado.ioloop
import tornado.httpclient
import tornado.options
import logging
import zlib

from streamsem import events
import streamsem.rdfevents

transferred_bytes = 0
data_count = 0

param_max_clients = 1000

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
        if self._started and not self._stopped:
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

    def start(self, loop=False):
        self.http_client = tornado.httpclient.AsyncHTTPClient(\
                                                max_clients=param_max_clients)
        req = tornado.httpclient.HTTPRequest(
            self.url, streaming_callback=self._stream_callback,
            request_timeout=None, connect_timeout=None)
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

    def _notify_event(self, data):
        global transferred_bytes
        transferred_bytes += len(data)
        evs = self._deserialize(data, parse_body=self.parse_event_body)
        if self.event_callback is not None:
            if not self.separate_events:
                self.event_callback(evs)
            else:
                for ev in evs:
                    self.event_callback(ev)

    def _stream_callback(self, data):
        self._notify_event(data)

    def _request_callback(self, response):
        if response.error:
            if self.error_callback is not None:
                self.error_callback('Error in HTTP request',
                                    http_error=response.error)
        elif len(response.body) > 0:
            self._notify_event(response.body)
        logging.info('Connection closed by server')
        if self._looping:
            self.ioloop.stop()
            self._looping = False
        if self.connection_close_callback:
            self.connection_close_callback(self)

    def _reset_compression(self):
        self._compressed = True
        self._decompresser = zlib.decompressobj()

    def _deserialize(self, data, parse_body=True):
        global data_count
        evs = []
        pos = 0
        if self._compressed:
            data = self._decompresser.decompress(data)
        data_count += len(data)
        while pos < len(data):
            event, pos = events.Event._deserialize_event(data, pos, parse_body)
            if isinstance(event, events.Command):
                if event.command == 'Set-Compression':
                    self._reset_compression()
                    evs.extend(self._deserialize(data[pos:], parse_body))
                    return evs
            else:
                evs.append(event)
#        logging.info('Transferred data: %d/%d (ratio %.3f)'%(transferred_bytes,#                                                            data_count,
#                                                            float(transferred_bytes)/data_count))
        return evs


def read_cmd_options():
    from optparse import OptionParser
    parser = OptionParser(usage='usage: %prog [options] stream_url',
                          version='0.0')
    (options, args) = parser.parse_args()
    if len(args) == 1:
        options.stream_url = args[0]
    else:
        parser.error('Stream URL required')
    return options

def read_cmd_options():
    from optparse import OptionParser, Values
    parser = OptionParser(usage='usage: %prog [options] source_stream_urls',
                          version='0.0')
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 1:
        options.stream_urls = remaining
    else:
        parser.error('At least one source stream URL required')
    return options

def main():
    import time
    def handle_event(event):
        print event
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
    client.start(loop=True)

if __name__ == "__main__":
    main()
