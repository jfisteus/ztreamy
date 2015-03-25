from __future__ import print_function

import time
import random

import ztreamy
import tornado.ioloop

# Create a server with two streams
server = ztreamy.StreamServer(9000)

# Create the streams; stream1 allows remote producers to publish through HTTP
stream1 = ztreamy.Stream('/stream1', allow_publish=True)
stream2 = ztreamy.Stream('/stream2')
server.add_stream(stream1)
server.add_stream(stream2)

# Create two publisher objects
publisher1 = ztreamy.LocalEventPublisher(stream1)
publisher2 = ztreamy.LocalEventPublisher(stream2)
source_id = ztreamy.random_id()
application_ids = ['ztreamy-example-a', 'ztreamy-example-b']

# Publish events periodically
def publish_hi():
    print('Publishing "hi"')
    app_id = random.choice(application_ids)
    event = ztreamy.Event(source_id, 'text/plain', 'Hi', application_id=app_id)
    publisher1.publish(event)

def publish_there():
    print('Publishing "there"')
    app_id = random.choice(application_ids)
    event = ztreamy.Event(source_id, 'text/plain', 'there!',
                          application_id=app_id)
    publisher2.publish(event)

tornado.ioloop.PeriodicCallback(publish_hi, 10000).start()
time.sleep(5)
tornado.ioloop.PeriodicCallback(publish_there, 10000).start()

try:
    print('Starting the server')
    server.start(loop=True)
except KeyboardInterrupt:
    # Allow ctrl-c to close the server
    pass
finally:
    server.stop()
