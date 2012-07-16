import sys

from streamsem.client import SynchronousClient

def main():
    if len(sys.argv) != 2:
        print('One parameter expected: stream URL')
        return
    url = sys.argv[1]
    client = SynchronousClient(url, parse_event_body=False)
    while True:
        events = client.receive_events()
        for event in events:
            print str(event)
            print

if __name__ == "__main__":
    main()
