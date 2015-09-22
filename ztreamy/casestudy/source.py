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
from rdflib.namespace import Namespace
from rdflib.graph import Graph
from rdflib import RDF
import rdflib.term as term
import time
import gzip
import tornado.options
import tornado.ioloop
import re

import ztreamy
import ztreamy.client as client
import ztreamy.logger as logger
from ztreamy.tools import utils
from ztreamy import ZtreamyException

ns_slog = Namespace('http://www.it.uc3m.es/jaf/ns/slog/#')
ns_person = Namespace('http://www.it.uc3m.es/jaf/ns/slog/person#')
ns_command = Namespace('http://www.it.uc3m.es/jaf/ns/slog/command#')

class LogEntry(object):
    _re_clean = re.compile('\<|\>')
    _date_format = "%Y-%m-%d %H:%M:%S"
    _entry_types = {
        'BA': ns_slog['event_shell'],
        'CC': ns_slog['event_compile'],
        'DB': ns_slog['event_debug_begin'],
        'DE': ns_slog['event_debug_end'],
        'IB': ns_slog['event_framework_begin'],
        'IE': ns_slog['event_framework_end'],
        'PO': ns_slog['event_post'],
        'TB': ns_slog['event_editor_begin'],
        'TE': ns_slog['event_editor_end'],
        'UR': ns_slog['event_visit_url'],
        'VB': ns_slog['event_valgrind_begin'],
        'VE': ns_slog['event_valgrind_end']
        }

    def __init__(self, seq_num, date, entry_type, subject, data1, data2):
        self.seq_num = int(seq_num)
        self.date = date
        self.timestamp = time.mktime(time.strptime(date, LogEntry._date_format))
        self.entry_type = entry_type
        self.subject = subject
        self.data1 = data1
        self.data2 = data2

    def __str__(self):
        return '%f: %s'%(self.date, self.entry_type)

    def graph(self):
        g = Graph()
        entry = term.BNode()
        g.add((entry, RDF.type, self._type_uri()))
        g.add((entry, ns_slog.timestamp, term.Literal(self.date)))
        g.add((entry, ns_slog.subject, ns_person[self.subject]))
        if self.entry_type == 'UR':
            g.add((entry, ns_slog.url, term.Literal(self.data2)))
        elif self.entry_type == 'BA':
            g.add((entry, ns_slog.command,
                   ns_command[self._escape(self.data1)]))
            g.add((entry, ns_slog.command_line, term.Literal(self.data2)))
        elif self.entry_type == 'PO':
            pass
        elif self.entry_type[0] == 'T' or self.entry_type[0] == 'I':
            g.add((entry, ns_slog.command_line, term.Literal(self.data2)))
        else:
            g.add((entry, ns_slog.num_lines, term.Literal(int(self.data1))))
            g.add((entry, ns_slog.command_line, term.Literal(self.data2)))
        return g

    def _type_uri(self):
        if self.entry_type in LogEntry._entry_types:
            return LogEntry._entry_types[self.entry_type]
        else:
            raise ZtreamyException('Unknown event type')

    def _escape(self, text):
        return LogEntry._re_clean.sub('', text)


class EventScheduler(object):
    def __init__(self, filename, io_loop, publishers,
                 time_scale, compressed=True):
        self.period = 10.0
        self.time_scale = time_scale
        self.publishers = publishers
        self.io_loop = io_loop
        self.source_ids = {}
        self.finished = False
        self.logfile_reader = self._read_logfile(filename,
                                                 compressed=compressed)
        self._pending_events = []
        self.sched = tornado.ioloop.PeriodicCallback(self._schedule_next_events,
                                                     self.period * 1000)
        self.sched.start()
        self._schedule_first_event()

    def _schedule_first_event(self):
        entry = self.logfile_reader.next()
        self.t0_original = entry.timestamp
        self.t0_new = time.time() + 5
        self._schedule_entry(entry)
        self._schedule_next_events()

    def _schedule_next_events(self):
        self._pending_events = [p for p in self._pending_events \
                                    if not p.finished]
        if not self.finished:
            try:
                limit = time.time() + 2 * self.period
                while True:
                    fire_time = self._schedule_entry(self.logfile_reader.next())
                    if fire_time > limit:
                        break
            except StopIteration:
                self.finished = True
        elif len(self._pending_events) == 0:
            self.sched.stop()
            self.io_loop.stop()

    def _schedule_entry(self, entry):
        if not entry.subject in self.source_ids:
            self.source_ids[entry.subject] = ztreamy.random_id()
        pub = utils.EventPublisher(entry, self.source_ids[entry.subject],
                                   self.publishers)
        fire_time = (self.t0_new
                     + (entry.timestamp - self.t0_original) / self.time_scale)
        self.io_loop.add_timeout(fire_time, pub.publish)
#        print entry.seq_num, 'scheduled for', fire_time
        return fire_time

    def _read_logfile(self, filename, compressed=True):
        if compressed:
            file_ = gzip.GzipFile(filename, 'r')
        else:
            file_ = open(filename, 'r')
        for line in file_:
            parts = [_strip(p) for p in line.split(',')]
            if parts[0] != 'n':
                parts = parts[0:5] + [','.join(parts[5:])]
                yield LogEntry(*parts)
        file_.close()


def _strip(data):
    data = data.strip()
    if len(data) >= 2 and data[0] == '"' and data[-1] == '"':
        data = data[1:-1]
    return data

def read_cmd_options():
    from optparse import OptionParser, Values
    tornado.options.define('eventlog', default=False,
                           help='dump event log',
                           type=bool)
    tornado.options.define('timescale', default=40000.0,
                           help='accelerate time by this factor',
                           type=float)
    remaining = tornado.options.parse_command_line()
    options = Values()
    if len(remaining) >= 1:
        options.server_urls = remaining
    else:
        OptionParser.error('At least one server URL required')
    return options

def main():
    options = read_cmd_options()
    entity_id = ztreamy.random_id()
    publishers = [client.EventPublisher(url) for url in options.server_urls]
    io_loop = tornado.ioloop.IOLoop.instance()
    filename = '../data-abel/EventData-sorted.csv.gz'
    scheduler = EventScheduler(filename, io_loop, publishers,
                               tornado.options.options.timescale,
                               compressed=True)
    if tornado.options.options.eventlog:
        logger.logger = logger.ZtreamyLogger(entity_id,
                                             'replay-' + entity_id + '.log')
    try:
        io_loop.start()
    except KeyboardInterrupt:
        pass
    finally:
        logger.logger.close()

if __name__ == "__main__":
    main()
