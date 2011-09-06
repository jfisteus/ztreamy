import tornado.options

import streamsem
from streamsem.server import StreamServer
from streamsem.client import AsyncStreamingClient

def read_cmd_options():
    from optparse import OptionParser, Values
    parser = OptionParser(usage='usage: %prog [options] source_stream_urls',
                          version='0.0')
    import tornado.options
    tornado.options.define('port', default=8888, help='run on the given port',
                           type=int)
    tornado.options.define('aggregatorid', default=None,
                           help='aggregator id', type=str)
    remaining = tornado.options.parse_command_line()
#    (options, args) = parser.parse_args()
    options = Values()
    if len(remaining) >= 1:
        options.stream_urls = remaining
    else:
        parser.error('At least one source stream URL required')
    return options

def main():
    options = read_cmd_options()
    server = StreamServer(tornado.options.options.port)
    if tornado.options.options.aggregatorid is None:
        tornado.options.options.aggregatorid = streamsem.random_id()
    def relay_event(event):
        event.append_aggregator_id(tornado.options.options.aggregatorid)
        server.dispatch_event(event)
    def handle_error(message, http_error=None):
        if http_error is not None:
            print message + ': ' + str(http_error)
        else:
            print message
    clients = [AsyncStreamingClient(url, event_callback=relay_event,
                                    error_callback=handle_error) \
                   for url in options.stream_urls]
    for client in clients:
        client.start(loop=False)
    server.start()

if __name__ == "__main__":
    main()
