from __future__ import print_function

import sys

from ztreamy.client import SynchronousClient

def main():
    if len(sys.argv) != 2:
        print('One parameter expected: stream URL')
        return
    url = sys.argv[1]
    client = SynchronousClient(url, parse_event_body=False)
    while not client.stream_finished:
        events = client.receive_events()
        for event in events:
            print(str(event))
            print()

if __name__ == "__main__":
    main()
