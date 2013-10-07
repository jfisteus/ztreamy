from __future__ import print_function

from ztreamy import SynchronousClient

stream = 'http://localhost:9000/stream1/long-polling'

client = SynchronousClient(stream)
try:
    while not client.stream_finished:
        events = client.receive_events()
        for event in events:
            print(str(event))
except KeyboardInterrupt:
    # Ctrl-c finishes the program
    pass
