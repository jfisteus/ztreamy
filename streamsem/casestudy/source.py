from rdflib.namespace import Namespace
from rdflib.graph import Graph
from rdflib import RDF
import rdflib.term as term
import time
import csv
import gzip

import streamsem
import streamsem.rdfevents as rdfevents

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


def read_logfile(filename, compressed=True):
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

def main():
    source_ids = {}
    filename = '../data-abel/EventData.csv.gz'
    for entry in read_logfile(filename, compressed=True):
        if not entry.subject in source_ids:
            source_ids[entry.subject] = streamsem.random_id()
        source_id = source_ids[entry.subject]
        e = rdfevents.RDFEvent(source_id,'n3', entry.graph())
        print e

if __name__ == "__main__":
    main()
