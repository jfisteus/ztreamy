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
import time
import random
import math
import sys
import logging
import tornado.ioloop

import ztreamy
from ztreamy import ZtreamyException
from ztreamy import events
from ztreamy import logger

class EventPublisher(object):
    def __init__(self, event, publishers, add_timestamp=False, ioloop=None):
        self.event = event
        self.publishers = publishers
        self.add_timestamp = add_timestamp
        self.finished = False
        self.error = False
        self._num_pending = 0
        self._external_callback = None
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()

    def publish(self):
        if self.add_timestamp:
            self.event.set_extra_header('X-Float-Timestamp',
                                        (str(self.event.sequence_num) + '/'
                                         + "%.3f"%time.time()))
        self._num_pending = len(self.publishers)
        for publisher in self.publishers:
            publisher.publish(self.event, self._callback)

    def set_external_callback(self, callback):
        self._external_callback = callback

    def _callback(self, response):
        self._num_pending -= 1
        if self._num_pending == 0:
            self.finished = True
            if self._external_callback is not None:
                self._external_callback()
        if response.error:
            self.error = True
            logging.error(response.error)
        ## else:
        ##     logging.info('Event successfully sent to server')


class _FakeResponse(object):
    """Class used from StdoutPublisher to simulate a Tornado HTTP response."""
    def __init__(self, error=False):
        self.error = error


class StdoutPublisher(object):
    """Simulates the interface of client.EventPublisher to write to stdout.

    Useful mainly for sending the serialized events through a pipe to
    other processes.

    """
    def __init__(self, ioloop=None):
        self.ioloop = ioloop or tornado.ioloop.IOLoop.instance()

    def publish(self, event, callback=None):
        """Publishes a new event."""
        logger.logger.event_published(event)
        body = ztreamy.serialize_events([event])
        sys.stdout.write(body)
        sys.stdout.flush()
        if callback is not None:
            def new_callback():
                callback(_FakeResponse())
            self.ioloop.add_callback(new_callback)

    def close(self):
        """Closes the event publisher."""
        pass


class EventScheduler(object):
    def __init__(self, source_id, io_loop, publishers, time_scale,
                 event_generator, time_generator=None, add_timestamp=False,
                 initial_delay=2.0):
        """ Schedules the events of the given file and sends.

        `source_id`: identifier to be set in command events generated
        by the scheduler.  `io_loop`: instance of the ioloop to use.
        `publishers`: list of EventPublisher objects.  `time_scale`:
        factor to accelerate time (used only when time_distribution is
        None.). `time_generator`: if not None, override times in the
        events and use the given interator as a source of event fire
        times. If None, events are sent according to the timestamps
        they have.

        """
        self.period = 10.0
        self.source_id = source_id
        self.time_scale = time_scale
        self.publishers = publishers
        self.io_loop = io_loop
        self.add_timestamp = add_timestamp
        self.finished = False
        self._pending_events = []
        self._event_generator = event_generator
        self._time_generator = time_generator
        self.initial_delay = initial_delay
        self.sched = tornado.ioloop.PeriodicCallback(self._schedule_next_events,
                                                     self.period * 1000)
        self.sched.start()
        self._schedule_first_event()

    def _schedule_first_event(self):
        self._send_init_event()
        event = self._event_generator.next()
        self.t0_original = event.time()
        self.t0_new = time.time() + 2 + self.initial_delay
        self._schedule_event(event)
        self._schedule_next_events()

    def _schedule_next_events(self):
        self._pending_events = [p for p in self._pending_events \
                                    if not p.finished]
        if not self.finished:
            try:
                limit = time.time() + 2 * self.period
                while True:
                    fire_time = self._schedule_event( \
                        self._event_generator.next())
                    if fire_time > limit:
                        break
            except StopIteration:
                self.finished = True
                if len(self._pending_events) > 0:
                    for event in self._pending_events:
                        event.set_external_callback(self._check_if_finished)
                else:
                    self._send_closing_event()
        elif len(self._pending_events) == 0:
            self.sched.stop()
            self.io_loop.stop()

    def _schedule_event(self, event):
        pub = EventPublisher(event, self.publishers,
                             add_timestamp=self.add_timestamp)
        self._pending_events.append(pub)
        if self._time_generator is None:
            fire_time = (self.t0_new
                         + (event.time() - self.t0_original) / self.time_scale)
        else:
            fire_time = self._time_generator.next()
        self.io_loop.add_timeout(fire_time, pub.publish)
        return fire_time

    def _check_if_finished(self):
        self._pending_events = [p for p in self._pending_events \
                                if not p.finished]
        if len(self._pending_events) == 0:
            self._send_closing_event()

    def _send_closing_event(self):
#        time.sleep(0.5)
        event = events.Command(self.source_id, 'ztreamy-command',
                               'Event-Source-Finished')
        pub = EventPublisher(event, self.publishers, add_timestamp=False)
        self._pending_events.append(pub)
        pub.publish()

    def _send_init_event(self):
        event = events.Command(self.source_id, 'ztreamy-command',
                               'Event-Source-Started')
        pub = EventPublisher(event, self.publishers, add_timestamp=False)
        self._pending_events.append(pub)
        self.io_loop.add_timeout(time.time() + self.initial_delay, pub.publish)


def exponential_event_scheduler(mean_time, initial_delay=0.0):
    last = time.time() + initial_delay
    while True:
        last += random.expovariate(1.0 / mean_time)
        yield last

def constant_event_scheduler(mean_time, initial_delay=0.0):
    last = time.time() + initial_delay
    while True:
        last += mean_time
        yield last

def get_scheduler(description, initial_delay=0.0):
    pos = description.find('[')
    if pos == -1 or description[-1] != ']':
        raise ZtreamyException('error in distribution specification',
                               'event_source params')
    distribution = description[:pos].strip()
    params = [float(num) for num in description[pos + 1:-1].split(',')]
    if distribution == 'exp':
        if len(params) != 1:
            raise ZtreamyException('exp distribution needs 1 param',
                                   'event_source params')
        return exponential_event_scheduler(params[0],
                                           initial_delay=initial_delay)
    elif distribution == 'const':
        if len(params) != 1:
            raise ZtreamyException('const distribution needs 1 param',
                                   'event_source params')
        return constant_event_scheduler(params[0],
                                        initial_delay=initial_delay)

def median(data):
    """Returns the statistic median of a list of values."""
    data = sorted(data)
    n = len(data)
    if n % 2 == 0:
        return float(data[n // 2 - 1] + data[n // 2]) / 2
    else:
        return float(data[n // 2])

def average_and_std_dev(data):
    """Returns the average and sample standard deviation of data."""
    n = len(data)
    total = sum(data)
    average = float(total) / n
    std_dev = math.sqrt((sum([(d - average) * (d - average) for d in data])
                         / (n - 1)))
    return average, std_dev
