import time

class StreamsemDefaultLogger(object):
    def __init__(self):
        self.log_file = None

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

    def _log(self, parts):
        self.log_file.write('\t'.join(parts))
        self.log_file.write('\n')


class StreamsemLogger(StreamsemDefaultLogger):
    def __init__(self, node_id, filename):
        self.log_file = open(filename, 'a')
        self.log_file.write('# Node: %s\n#\n'%node_id)

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


class StreamsemManycLogger(StreamsemDefaultLogger):
    def __init__(self, node_id, filename):
        self.log_file = open(filename, 'a')
        self.log_file.write('# Node: %s\n#\n'%node_id)

    def manyc_event_finished(self, sequence_num, delays):
        parts = ['manyc_event_finish', str(sequence_num)]
        parts.extend([str(delay) for delay in delays])
        self._log(parts)


def timestamp():
    return '%.6f'%time.time()

# Default logger
logger = StreamsemDefaultLogger()
