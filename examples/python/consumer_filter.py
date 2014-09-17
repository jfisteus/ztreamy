from __future__ import print_function

from ztreamy import Client, ApplicationFilter

def event_received(event):
    print(str(event))
    print()

def error(message, http_error=None):
    if http_error is not None:
        print('[Error] ' + message + ': ' + str(http_error))
    else:
        print('[Error] ' + message)

streams = [
    'http://localhost:9000/stream1/compressed',
    'http://localhost:9000/stream2/compressed',
    ]

filter_ = ApplicationFilter(event_received, application_id='ztreamy-example-a')
client = Client(streams, event_callback=filter_, error_callback=error)
try:
    # Start receiving events and block on the IOLoop
    client.start(loop=True)
except KeyboardInterrupt:
    # Ctrl-c finishes the program
    pass
finally:
    client.stop()
