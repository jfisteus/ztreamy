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
from __future__ import print_function

import tornado.ioloop
import logging
import time
import datetime
import random

import ztreamy
from ztreamy import client
from ztreamy import logger
from ztreamy import events

class BogusDeserializer(object):
    tokens = ['X-Float-Timestamp:', 'Event-Source-Finished']

    def __init__(self):
        """Creates a new `Deserializer` object."""
        self.reset()

    def reset(self):
        """This method resets the state of the parser and dumps pending data"""
        self.parse_state = [-1, -1, -1]
        self.param_data = ''

    def deserialize(self, data):
        """Deserializes and returns a list of events.

        Deserializes all the events until no more events can be parsed.

        """
        evs = []
        ev = self._match_token(0, data, 0)
        if ev is not None:
            evs.append(ev)
        ev = self._match_token(1, data, 0)
        if ev is not None:
            evs.append(ev)
            return evs
        pos = data.find('\n')
        while pos != -1:
            self.parse_state = [0, 0, 0]
            ev = self._match_token(0, data, pos + 1)
            if ev is not None:
                evs.append(ev)
            ev = self._match_token(1, data, pos + 1)
            if ev is not None:
                evs.append(ev)
                return evs
            pos = data.find('\n', pos + 1)
        return evs

    def _match_token(self, token, data, ini):
        pos = ini
        if self.parse_state[token] >= 0:
            end = len(BogusDeserializer.tokens[token])
            if end > len(data) - ini + self.parse_state[token]:
                end = len(data) - ini + self.parse_state[token]
            text = BogusDeserializer.tokens[token][self.parse_state[token]:end]
            if data[ini:ini + end - self.parse_state[token]] == text:
                pos = ini + end - self.parse_state[token]
                self.parse_state[token] = end
                if self.parse_state[token] == \
                        len(BogusDeserializer.tokens[token]):
                    self.parse_state = [-1, -1, -1]
                    self.parse_state[token] = -2
            else:
                self.parse_state[token] = -1
        if token == 0 and self.parse_state[0] == -2:
            end = data.find('\n', pos)
            if end == -1:
                self.param_data += data[pos:]
            else:
                self.param_data += data[pos:end]
                parts = self.param_data.split('/')
                self.reset()
                return int(parts[0]), float(parts[1])
        elif token == 1 and self.parse_state[1] == -2:
            self.reset()
            self.consumed_data = pos
            return events.Command('', 'ztreamy-command',
                                  'Event-Source-Finished')
        return None


class BogusClient(client.AsyncStreamingClient):
    """A quick client that only scans for the key data needed.

    It should allow increasing CPU efficiency when using many clients.

    """

    def __init__(self, url, stats, no_parse, ioloop=None, close_callback=None,
                 finish_callback=None):
        super(BogusClient, self).__init__(url, ioloop=ioloop,
                                    connection_close_callback=close_callback,
                                    reconnect=False)
        self.stats = stats
        self._deserializer = BogusDeserializer()
        self.no_parse = no_parse
        if no_parse:
            self._stream_callback = None
            self._reset_compression = self._reset_compression_no_parse
        else:
            self._deserializer = BogusDeserializer()
        self.finished = False
        self.finish_callback = finish_callback

    def _stream_callback(self, data):
        evs = self._deserialize(data)
        for e in evs:
            self.stats.handle_event(e)

    def _deserialize(self, data):
        evs = []
        logger.logger.data_received(len(data), len(data))
        evs = self._deserializer.deserialize(data)
        if len(evs) > 0 and isinstance(evs[-1], events.Command):
            if evs[-1].command == 'Event-Source-Finished':
                pos = self._deserializer.consumed_data
                self._deserializer.reset()
                self.finished = True
                del evs[-1]
                evs.extend(self._deserialize(data[pos:]))
                if self.finish_callback is not None:
                    self.finish_callback()
                self.stop()
        return evs

    def _reset_compression_no_parse(self):
        pass


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
        delay = time.time() - event[1]
        self.sum_delays += delay
        if delay > self.max_delay:
            self.max_delay = delay
        if delay < self.min_delay:
            self.min_delay = delay
        self.num_events_received += 1
        self.pending.event_received(event[0], delay)

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

    def event_received(self, sequence_num, delay):
        if sequence_num in self.unfinished:
            self.unfinished[sequence_num] += 1
            self.delays[sequence_num].append(delay)
        else:
            for i in range(self.most_recent + 1, sequence_num + 1):
                self.unfinished[i] = 0
                self.delays[i] = []
            self.unfinished[sequence_num] = 1
            self.delays[sequence_num].append(delay)
            if sequence_num > self.most_recent:
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


class SaturationMonitor(object):
    def __init__(self, period, clients):
        self.period = period
        self.clients = clients
        self.last_fire = None
        self.delayed = False

    def fire(self):
        now = time.time()
        if self.last_fire is not None:
            diff = now - self.last_fire
            if diff > 1.2 * self.period:
                if not self.delayed:
                    self.delayed = True
                    logging.info('In delay')
            elif self.delayed:
                self.delayed = False
                logging.info('Normal operation again')


def _invoke_later(function):
    t = datetime.timedelta(seconds=1 + random.expovariate(1))
    tornado.ioloop.IOLoop.instance().add_timeout(t, function)

def read_cmd_options():
    from optparse import OptionParser, Values
    tornado.options.define('eventlog', default=False,
                           help='dump event log',
                           type=bool)
    tornado.options.define('noparse', default=False,
                           help='quick client that does not parse events',
                           type=bool)
    tornado.options.define('reconnect', default=False,
                           help='reconnect disconnected clients',
                           type=bool)
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) == 2:
        options.stream_url = remaining[0]
        options.num_clients = int(remaining[1])
    else:
        OptionParser().error('A source stream URL required')
    return options

def main():
    def close_callback(cli):
        reconnecting = False
        clients.remove(cli)
        if tornado.options.options.reconnect and not cli.finished:
            if times_reconnected[0] >= max_reconnections:
                print('Active clients: {} / {}'.format(len(clients), options.num_clients))
                num_disconnected_clients[0] += 1
                print('A client got disconnected with times overflown {}'.format(entity_id))
            else:
                print('preparing reconnection {}'.format(entity_id))
                reconnecting = True
                times_reconnected[0] += 1
                if cli.no_parse:
                    _invoke_later(connect_new_client_no_parsing)
                else:
                    _invoke_later(connect_new_client_parsing)
        elif not cli.finished:
            num_disconnected_clients[0] += 1
            print('A client got disconnected with reconnect disabled {}'.format(entity_id))
        if len(clients) == 0 and not reconnecting:
            tornado.ioloop.IOLoop.instance().stop()

    def connect_new_client_no_parsing():
        times_reconnected[0] += 1
        new_client = BogusClient(options.stream_url, stats, True,
                                 close_callback=close_callback)
        clients.append(new_client)
        new_client.start(loop=False)
        print('Created a new non-parsing client for reconnection: {}'.format(entity_id))

    def connect_new_client_parsing():
        times_reconnected[0] += 1
        new_client = BogusClient(options.stream_url, stats, False,
                                 close_callback=close_callback,
                                 finish_callback=finish_callback)
        clients.append(new_client)
        new_client.start(loop=False)
        print('Created parsing client for reconnection {}'.format(entity_id))

    def finish_callback():
        for cli in clients:
            cli.finished = True
            cli.stop(notify_connection_close=False)
        del clients[:]
        tornado.ioloop.IOLoop.instance().stop()

    options = read_cmd_options()
    no_parse = tornado.options.options.noparse
    assert options.num_clients > 0
    entity_id = ztreamy.random_id()
    num_disconnected_clients = [0]
    times_reconnected = [0]
    max_reconnections = 3 * options.num_clients
    if max_reconnections < 100:
        max_reconnections = 100
    client.configure_max_clients(options.num_clients)
    clients = []
    if not no_parse:
        stats = _Stats(options.num_clients)
        for i in range(0, options.num_clients):
            clients.append(BogusClient(options.stream_url, stats, False,
                                       close_callback=close_callback))
    else:
        # One client parses, and the rest don't
        stats = _Stats(1)
        for i in range(0, options.num_clients - 1):
            clients.append(BogusClient(options.stream_url, stats, True,
                                       close_callback=close_callback))
        clients.append(BogusClient(options.stream_url, stats, False,
                                   close_callback=close_callback,
                                   finish_callback=finish_callback))
    for c in clients:
        c.start(loop=False)
    sched = tornado.ioloop.PeriodicCallback(stats.log_stats, 5000)
    if no_parse:
        saturation_mon = SaturationMonitor(5.0, clients)
        sched = tornado.ioloop.PeriodicCallback(saturation_mon.fire, 5000)
    sched.start()
    if tornado.options.options.eventlog:
        print(entity_id)
        logger.logger = logger.ZtreamyManycLogger(entity_id,
                                                'manyc-' + entity_id + '.log')
        logger.logger.auto_flush = True
    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
    finally:
        for c in clients:
            c.stop()
        if num_disconnected_clients[0] > 0:
            logging.warning((str(num_disconnected_clients[0])
                             + ' clients got disconnected'))
        logger.logger.close()

if __name__ == "__main__":
    main()
