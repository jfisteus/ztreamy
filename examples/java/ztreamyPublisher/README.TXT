This directory contains example code that sends test events for
publication through a Ztreamy stream. Run it as:

java Publisher <stream publication URI> <number of events> [data log file]

For instance, the following command generates 3 events and sends them
to a local Ztreamy instance that listens at port 9000. It logs the
serializations of the events to the "log.txt" file:

java Publisher http://localhost:9000/events/publish 3 log.txt

The program requires the Google Gson library for JSON support:

https://code.google.com/p/google-gson/

You need to download its JAR file and place it in your CLASSPATH
in order to run this example.

If you want to use this code from Android, you'll probably want to
write an alternative JSONSerializer class that uses the JSON
support already provided by Android. If you do that, I would appreciate
it if you contributed your implementation back, so that I can
make it available with the rest of this code.
