import tornado.ioloop
import tornado.httpclient
import logging

from streamsem import events

class AsyncStreamingClient(object):
    def __init__(self, url, event_callback=None, error_callback=None,
                 ioloop=None, parse_event_body=True):
        self.url = url
        self.event_callback = event_callback
        self.error_callback = error_callback
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()
        self.parse_event_body = parse_event_body
        self._closed = False
        self._looping = False

    def start(self, loop=False):
        self.http_client = tornado.httpclient.AsyncHTTPClient()
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
        event = events.deserialize(data, parse_body=self.parse_event_body)
        if event is not None:
            if self.event_callback is not None:
                self.event_callback(event)
        else:
            if self.error_callback is not None:
                self.error_callback('Error while deserializing event')

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
    client = AsyncStreamingClient(options.stream_url,
                                  event_callback=handle_event,
                                  error_callback=handle_error)
#    tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 6, stop_client)
    client.start(loop=True)

if __name__ == "__main__":
    main()
