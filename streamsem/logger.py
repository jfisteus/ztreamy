import time

class StreamsemLogger(object):
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

    def _log(self, parts):
        self.log_file.write('\t'.join(parts))
        self.log_file.write('\n')


class StreamsemLoggerStub(object):
    def close(self):
        pass

    def event_created(self, event):
        pass

    def event_dispatched(self, event):
        pass

    def event_delivered(self, event):
        pass


def timestamp():
    return '%.6f'%time.time()

# Default logger
logger = StreamsemLoggerStub()
