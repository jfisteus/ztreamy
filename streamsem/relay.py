import tornado.options

from streamsem.server import RelayServer

def read_cmd_options():
    from optparse import Values
    tornado.options.define('port', default=8888, help='run on the given port',
                           type=int)
    tornado.options.define('aggregatorid', default=None,
                           help='aggregator id', type=str)
    tornado.options.define('buffer', default=None, help='event buffer time (s)',
                           type=float)
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 1:
        options.stream_urls = remaining
    else:
        parser.error('At least one source stream URL required')
    return options

def main():
    import time
    options = read_cmd_options()
    def stop_server():
        server.stop()
#    import streamsem.filters
#    filter_=streamsem.filters.SourceFilter(None,
#                             source_id='65f0bfeb-cc79-4188-8404-175f3a6be6c3')
    if tornado.options.options.buffer is not None:
        buffering_time = tornado.options.options.buffer * 1000
    else:
        buffering_time = None
    server = RelayServer(tornado.options.options.port, options.stream_urls,
                         buffering_time=buffering_time)

    # Uncomment to test RelayServer.stop():
#    tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 5, stop_server)

    server.start()

if __name__ == "__main__":
    main()
