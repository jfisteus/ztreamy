""" Analizes log files to obtain performance statistics.

"""

class Analyzer(object):
    def __init__(self):
        self.events = {}
        self.data_delivery_log = DataDeliveryLog()

    def register_publish(self, event_id, publish_time):
        event = self._get_or_create(event_id)
        event.publish_times.append(publish_time)

    def register_dispatch(self, event_id, dispatch_time):
        event = self._get_or_create(event_id)
        event.dispatch_times.append(dispatch_time)

    def register_deliver(self, event_id, deliver_time):
        event = self._get_or_create(event_id)
        event.deliver_times.append(deliver_time)

    def register_data_delivery(self, compressed, uncompressed):
        self.data_delivery_log.register_data_delivery(compressed, uncompressed)

    def parse_file(self, filename):
        with open(filename) as file_:
            for line in file_:
                line = line.strip()
                if line.startswith('#') or line == '':
                    continue
                parts = [part.strip() for part in line.split('\t')]
                if parts[0] == 'event_publish':
                    assert len(parts) == 3
                    self.register_publish(parts[1], float(parts[2]))
                elif parts[0] == 'event_dispatch':
                    assert len(parts) == 3
                    self.register_dispatch(parts[1], float(parts[2]))
                elif parts[0] == 'event_deliver':
                    assert len(parts) == 3
                    self.register_deliver(parts[1], float(parts[2]))
                elif parts[0] == 'data_receive':
                    assert len(parts) == 3
                    self.register_data_delivery(int(parts[1]), int(parts[2]))

    def _get_or_create(self, event_id):
        if not event_id in self.events:
            self.events[event_id] = EventLog(event_id)
        return self.events[event_id]


class EventLog(object):
    def __init__(self, event_id):
        self.event_id = event_id
        self.publish_times = []
        self.dispatch_times = []
        self.deliver_times = []

    def register_publish(self, publish_time):
        self.publish_times.append(publish_time)

    def register_dispatch(self, dispatch_time):
        self.dispatch_times.append(dispatch_time)

    def register_deliver(self, deliver_time):
        self.deliver_times.append(deliver_time)

    def analyze(self):
        self.publish_times.sort()
        self.dispatch_times.sort()
        self.delivery_delays = [deliver - self.publish_times[0] \
                                    for deliver in self.deliver_times]


class DataDeliveryLog(object):
    def __init__(self):
        self.uncompressed_data = 0
        self.compressed_data = 0

    def register_data_delivery(self, compressed, uncompressed):
        self.compressed_data += compressed
        self.uncompressed_data += uncompressed


def main():
    pass

if __name__ == "__main__":
    main()
