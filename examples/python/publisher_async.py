from __future__ import print_function

import ztreamy
import tornado.ioloop

# Create a publisher object
stream = 'http://localhost:9000/stream1'
publisher = ztreamy.EventPublisher(stream)
source_id = ztreamy.random_id()

# Publish events periodically
def publish():
    print('Publishing')
    event = ztreamy.Event(source_id, 'text/plain',  'This is a new event')
    publisher.publish(event)

tornado.ioloop.PeriodicCallback(publish, 10000).start()

try:
    # Block on the ioloop
    tornado.ioloop.IOLoop.instance().start()
except KeyboardInterrupt:
    # Allow ctrl-c to finish the program
    pass
finally:
    publisher.close()
