This directory contains example code that subscribes to a Ztreamy
stream in long-lived mode and prints the raw contents of the stream
to stdout. Run it as:

java Consumer <stream URI>

For instance, the following command subscribes to a stream published
by a local Ztreamy server at port 9000:

java Publisher http://localhost:9000/events/stream

The program continues running until the server stops or you
interrupt it with CTRL-C.
