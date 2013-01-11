import time

import ztreamy
import tornado.ioloop

# Create a server with two streams
server = ztreamy.StreamServer(9000)
stream1 = ztreamy.Stream('/stream1')
stream2 = ztreamy.Stream('/stream2')
server.add_stream(stream1)
server.add_stream(stream2)

# Create two publisher objects
publisher1 = ztreamy.LocalEventPublisher(stream1)
publisher2 = ztreamy.LocalEventPublisher(stream2)
source_id = ztreamy.random_id()

# Publish events periodically
def publish_hi():
    print 'Publishing "hi"'
    event = ztreamy.Event(source_id, 'text/plain', 'Hi')
    publisher1.publish(event)

def publish_there():
    print 'Publishing "there"'
    event = ztreamy.Event(source_id, 'text/plain', 'there!')
    publisher2.publish(event)

tornado.ioloop.PeriodicCallback(publish_hi, 10000).start()
time.sleep(5)
tornado.ioloop.PeriodicCallback(publish_there, 10000).start()

try:
    print 'Starting the server'
    server.start(loop=True)
except KeyboardInterrupt:
    # Allow ctrl-c to close the server
    pass
finally:
    server.stop()
