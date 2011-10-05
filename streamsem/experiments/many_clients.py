import tornado.ioloop
import logging

from streamsem import client

class _Stats(object):
    def __init__(self):
        self.num_events_received = 0

    def handle_event(self, event):
        self.num_events_received += 1

    def handle_error(self, message, http_error=None):
        if http_error is not None:
            logging.error(message + ': ' + str(http_error))
        else:
            logging.error(message)

    def __str__(self):
        return 'Events received: %d'%self.num_events_received

    def log_stats(self):
        logging.info(str(self))


def read_cmd_options():
    from optparse import OptionParser, Values
    parser = OptionParser(usage='usage: %prog [options] source_stream_url num',
                          version='0.0')
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) == 2:
        options.stream_url = remaining[0]
        options.num_clients = int(remaining[1])
    else:
        parser.error('A source stream URL required')
    print options.stream_url, options.num_clients
    return options

def main():
    options = read_cmd_options()
    stats = _Stats()
    clients = []
    for i in range(0, options.num_clients):
        clients.append(client.Client([options.stream_url],
                                     event_callback=stats.handle_event,
                                     error_callback=stats.handle_error))
    for c in clients:
        c.start(loop=False)
    sched = tornado.ioloop.PeriodicCallback(stats.log_stats, 5000)
    sched.start()
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
