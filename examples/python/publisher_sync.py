import time

import ztreamy

# Create a publisher object
stream = 'http://localhost:9000/stream1'
publisher = ztreamy.SynchronousEventPublisher(stream)
source_id = ztreamy.random_id()

try:
    while True:
        time.sleep(10)
        event = ztreamy.Event(source_id, 'text/plain',  'This is a new event')
        publisher.publish(event)
except KeyboardInterrupt:
    # Allow ctrl-c to finish the program
    pass
finally:
    publisher.close()
