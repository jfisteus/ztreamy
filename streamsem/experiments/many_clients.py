import tornado.ioloop
import logging
import time

import streamsem
from streamsem import client
from streamsem import logger

class _Stats(object):
    def __init__(self, num_clients):
        self.reset_counters()
        self.pending = _PendingEvents(num_clients)

    def reset_counters(self):
        self.num_events_received = 0
        self.sum_delays = 0.0
        self.max_delay = 0.0
        self.min_delay = 1e99

    def handle_event(self, event):
        delay = time.time() - float(event.extra_headers['X-Float-Timestamp'])
        self.sum_delays += delay
        if delay > self.max_delay:
            self.max_delay = delay
        if delay < self.min_delay:
            self.min_delay = delay
        self.num_events_received += 1
        self.pending.event_received(event, delay)

    def handle_error(self, message, http_error=None):
        if http_error is not None:
            logging.error(message + ': ' + str(http_error))
        else:
            logging.error(message)

    def __str__(self):
        parts = ['Received: %d'%self.num_events_received]
        if self.num_events_received > 0:
            parts.append('Avg. delay: %.3f (%.3f, %.3f)'%\
                 (self.sum_delays / self.num_events_received,
                 self.min_delay,
                 self.max_delay))
        parts.append('Unf: %d, Fin: %d, Old: %d'%\
                         (self.pending.count_unfinished(),
                          self.pending.finished_events,
                          self.pending.oldest_unfinished()))
        return '; '.join(parts)

    def log_stats(self):
        logging.info(str(self))
        self.reset_counters()


class _PendingEvents(object):
    def __init__(self, num_clients):
        self.unfinished = {}
        self.delays = {}
        self.num_clients = num_clients
        self.finished_events = 0
        self.most_recent = 0

    def event_received(self, event, delay):
        sequence_num = int(event.extra_headers['X-Sequence-Num'])
        if sequence_num in self.unfinished:
            self.unfinished[sequence_num] += 1
            self.delays[sequence_num].append(delay)
        else:
            for i in range(self.most_recent + 1, sequence_num + 1):
                self.unfinished[i] = 0
                self.delays[i] = []
            self.unfinished[sequence_num] = 1
            self.delays[sequence_num].append(delay)
            self.most_recent = sequence_num
        if self.unfinished[sequence_num] == self.num_clients:
            logger.logger.manyc_event_finished(sequence_num,
                                               self.delays[sequence_num])
            del self.unfinished[sequence_num]
            del self.delays[sequence_num]
            self.finished_events += 1

    def count_unfinished(self):
        return len(self.unfinished)

    def oldest_unfinished(self):
        if len(self.unfinished) > 0:
            return min(self.unfinished)
        else:
            return 0


def read_cmd_options():
    from optparse import Values
    tornado.options.define('eventlog', default=False,
                           help='dump event log',
                           type=bool)
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
    entity_id = streamsem.random_id()
    stats = _Stats(options.num_clients)
    clients = []
    for i in range(0, options.num_clients):
        clients.append(client.Client([options.stream_url],
                                     event_callback=stats.handle_event,
                                     error_callback=stats.handle_error))
    for c in clients:
        c.start(loop=False)
    sched = tornado.ioloop.PeriodicCallback(stats.log_stats, 5000)
    sched.start()
    if tornado.options.options.eventlog:
        logger.logger = logger.StreamsemManycLogger(entity_id,
                                                'manyc-' + entity_id + '.log')
    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
    finally:
        for c in clients:
            c.stop()

if __name__ == "__main__":
    main()
