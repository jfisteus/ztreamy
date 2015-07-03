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
"""Loggers used by the framework, mainly for performance evaluation.

In principle, they are intended for internal use only.

"""

import time
from socket import gethostname

class ZtreamyDefaultLogger(object):
    def __init__(self):
        self.log_file = None
        self.auto_flush = False

    def close(self):
        pass

    def flush(self):
        pass

    def event_published(self, event):
        pass

    def event_dispatched(self, event):
        pass

    def event_delivered(self, event):
        pass

    def data_received(self, compressed, uncompressed):
        pass

    def manyc_event_finished(self, sequence_num, delays):
        pass

    def server_traffic_sent(self, timestamp, num_bytes):
        pass

    def server_closed(self, num_clients):
        pass

    def server_timing(self, cpu_time, real_time, init_time):
        pass

    def _open_file(self, node_id, filename):
        self.log_file = open(filename, 'a')
        self.log_file.write('# Node: %s\n# Host: %s\n#\n'%(node_id,
                                                           gethostname()))

    def _write_comments(self, dict_data):
        for key, value in dict_data.iteritems():
            self.log_file.write('# %s: %s\n'%(key, str(value)))

    def _log(self, parts):
        self.log_file.write('\t'.join(parts))
        self.log_file.write('\n')
        if self.auto_flush:
            self.log_file.flush()


class ZtreamyLogger(ZtreamyDefaultLogger):
    def __init__(self, node_id, filename):
        super(ZtreamyLogger, self).__init__()
        self._open_file(node_id, filename)

    def close(self):
        self.log_file.close()

    def flush(self):
        self.log_file.flush()

    def event_published(self, event):
        parts = ['event_publish', event.event_id, timestamp()]
        self._log(parts)

    def event_dispatched(self, event):
        parts = ['event_dispatch', event.event_id, timestamp()]
        self._log(parts)

    def event_delivered(self, event):
        parts = ['event_deliver', event.event_id, timestamp()]
        self._log(parts)

    def data_received(self, compressed, uncompressed):
        parts = ['data_receive', str(compressed), str(uncompressed)]
        self._log(parts)

    def manyc_event_finished(self, sequence_num, delays):
        parts = ['manyc_event_finish', str(sequence_num)]
        parts.extend([str(delay) for delay in delays])
        self._log(parts)


class ZtreamyManycLogger(ZtreamyDefaultLogger):
    def __init__(self, node_id, filename):
        super(ZtreamyManycLogger, self).__init__()
        self._open_file(node_id, filename)

    def data_received(self, compressed, uncompressed):
        parts = ['data_receive', str(compressed), str(uncompressed)]
        self._log(parts)

    def manyc_event_finished(self, sequence_num, delays):
        parts = ['manyc_event_finish', str(sequence_num)]
        parts.extend([str(delay) for delay in delays])
        self._log(parts)


class CompactServerLogger(ZtreamyDefaultLogger):
    def __init__(self, node_id, filename, comments):
        super(CompactServerLogger, self).__init__()
        self._open_file(node_id, filename)
        self._write_comments(comments)

    def server_closed(self, num_clients):
        parts = ['server_closed', str(time.time()), str(num_clients)]
        self._log(parts)

    def server_traffic_sent(self, timestamp, num_bytes):
        parts = ['server_traffic_sent', str(timestamp), str(num_bytes)]
        self._log(parts)

    def server_timing(self, cpu_time, real_time, init_time):
        parts = ['server_timing', str(cpu_time), str(real_time),
                 str(init_time)]
        self._log(parts)


def timestamp():
    return '%.6f'%time.time()

# Default logger
logger = ZtreamyDefaultLogger()
