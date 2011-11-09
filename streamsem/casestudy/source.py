from rdflib.namespace import Namespace
from rdflib.graph import Graph
from rdflib import RDF
import rdflib.term as term
import time
import csv
import gzip
import tornado.options
import tornado.ioloop

import streamsem
import streamsem.rdfevents as rdfevents
import streamsem.client as client

ns_slog = Namespace('http://www.it.uc3m.es/jaf/ns/slog/#')
ns_person = Namespace('http://www.it.uc3m.es/jaf/ns/slog/person#')

class LogEntry(object):
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
            g.add((entry, ns_slog.command, term.Literal(self.data1)))
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
            raise StreamsemException('Unknown event type')


class EventPublisher(object):
    def __init__(self, log_entry, source_id, publishers):
        self.log_entry = log_entry
        self.source_id = source_id
        self.publishers = publishers

    def publish(self):
        event = rdfevents.RDFEvent(self.source_id, 'n3',
                                   self.log_entry.graph())
        for publisher in self.publishers:
            publisher.publish(event)


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
        try:
            limit = time.time() + 2 * self.period
            while True:
                fire_time = self._schedule_entry(self.logfile_reader.next())
                if fire_time > limit:
                    break
        except StopIteration:
            self.sched.stop()
            self.finished = True

    def _schedule_entry(self, entry):
        if not entry.subject in self.source_ids:
            self.source_ids[entry.subject] = streamsem.random_id()
        pub = EventPublisher(entry, self.source_ids[entry.subject],
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


def _strip(data):
    data = data.strip()
    if len(data) >= 2 and data[0] == '"' and data[-1] == '"':
        data = data[1:-1]
    return data

def read_cmd_options():
    from optparse import Values
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
        parser.error('At least one server URL required')
    return options

def main():
    options = read_cmd_options()
    publishers = [client.EventPublisher(url) for url in options.server_urls]
    io_loop = tornado.ioloop.IOLoop.instance()
    filename = '../data-abel/EventData.csv.gz'
    scheduler = EventScheduler(filename, io_loop, publishers,
                               tornado.options.options.timescale,
                               compressed=True)
    if tornado.options.options.eventlog:
        logger.logger = logger.StreamsemLogger(source_id,
                                               'source-' + source_id + '.log')
    try:
        io_loop.start()
    except KeyboardInterrupt:
        pass
    finally:
        logger.logger.close()

if __name__ == "__main__":
    main()
