import tornado.options

from streamsem.server import RelayServer

def read_cmd_options():
    from optparse import OptionParser, Values
    parser = OptionParser(usage='usage: %prog [options] source_stream_urls',
                          version='0.0')
    tornado.options.define('port', default=8888, help='run on the given port',
                           type=int)
    tornado.options.define('aggregatorid', default=None,
                           help='aggregator id', type=str)
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
    server = RelayServer(tornado.options.options.port, options.stream_urls,
                         tornado.options.options.aggregatorid)

    # Uncomment to test RelayServer.stop():
#    tornado.ioloop.IOLoop.instance().add_timeout(time.time() + 5, stop_server)

    server.start()

if __name__ == "__main__":
    main()
