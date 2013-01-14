Programming applications in Ztreamy
===================================

:Author: Jesús Arias Fisteus

.. contents::
.. section-numbering::


Introduction
------------

This document describes how you can develop applications that consume
events, produce events (sensors) or act as streaming servers. Servers
must necessarily use the Python libraries we provide as part of
Ztreamy. Consumers and producers can be programmed in any programming
language. They just need to communicate with the streaming server with
HTTP. Nevertheless, we also provide a library that helps programming
those kinds of applications in Python.


Asynchronous programming vs. synchronous programming
----------------------------------------------------

Ztreamy is built on top of the `Tornado Web Server
<http://www.tornadoweb.org/>`_. Tornado's architecture is based on an
event loop they call `the IOLoop
<http://www.tornadoweb.org/documentation/ioloop.html>`_. The IOLoop is
in control of the process. Input/output operations (e.g. for sending
and receiving data through sockets) are non-blocking: they return
immediately instead of waiting for the operation to complete (new data
to arrive or the data to be sent to be effectively dispatched). They
register callback functions that the system calls when the operation
completes.


Consuming events
----------------

If your application needs to consume events, you have to decide
whether to program your application asynchronously (on top of
Tornado's IOLoop) or synchronously. The key functions of the
asynchronous library are non-blocking. On the contrary, all the
operations of the synchronous library are blocking. Asynchronous
consumers are developed by using the `ztreamy.client.Client` class,
whereas synchronous consumers use the
`ztreamy.client.SynchronousClient`.


Stream URIs
...........

Streams are identified by HTTP URIs. Because the same stream server
can serve more than one stream, a stream has an associated path. For
example, the following stream has the path `/stream1` at a stream
server installed at port 9000 of `example.com`::

    http://example.com:9000/stream1

When clients want to connect to a stream, the need to specify also an
access mode, which can be one of the following:

- Long-lived requests: a single HTTP request is issued when the client
  connects. The server sends data as new events are available, but it
  does not finish the response.

- Short-lived requests: every time the client wants more data, it
  issues a new HTTP request. The server returns the events that are
  new since the previous request of the client and finishes the
  request. If there are no new events, the server keeps the request on
  hold until at least one event is available.

The access mode is communicated to the server by appending to the path
of the URI one of the following components:

- `/compressed`: long-lived requests with ZLIB compression.

- `/stream`: long-lived requests with no compression.

- `/short-lived`: short lived requests with no compression.


Developing a consumer asynchronously
....................................

The following program connects to two streams and prints the events
that come from them:

    .. include:: ../examples/consumer_async.py
       :literal:

Using the client requires three main steps:

1. Creating a `Client` object. It receives a list of stream URIs as
   parameter. If you only want to connect to one stream, you can
   provide directly the string with the URI instead of a list. In
   addition, you should provide a callback function, that will be
   called by Ztreamy for each event received from any of the streams
   the client is connected to. Optionally, you can also provide a
   callback function to receive error notifications.

2. Starting the client with its `start` method. The library starts the
   process of connecting to the stream servers at this point.

3. Stopping the client at the end with its `stop` method. This method
   closes the connection to the stream servers and liberates the
   resources used by the client.

The callback function for received events is called by the library
whenever an event arrives. It receives the event (an instance of
`ztreamy.Event` or of one of its subclasses) as a parameter.

Note that the URIs of the streams must specify a long-lived requests
access mode: the stream name in the path must be followed either by
`/compressed` or by `/stream`. The difference between the two of them
is that the first one uses ZLIB compression. Your program does not
need to be aware about compression, because `ztreamy` decompresses the
data internally. Connecting to the compressed stream should normally
be the preferred option, due to the amount if traffic it saves.


Developing a consumer synchronously
...................................

The following program connects to a stream with the synchronous API
and prints the events that come from it:

    .. include:: ../examples/consumer_sync.py
       :literal:

There are two main steps:

1. Creating the `SynchronousClient` object. It receives a string with
   the URI of the stream.

2. Invoking repeatedly its method `receive_events`. It sends an HTTP
   request to the server in order to get all the new events that
   appeared after the previous request. The method returns immediately
   if there are new events. If not, the method blocks until there are
   new events or the server closes the connection.

Note that the URI of the stream must instruct the server to use the
short-lived requests mode: the stream name in the path must be
followed by `/short-lived`, like in the example.


Publishing events
-----------------

You can publish events from the stream server itself, or from a remote
client that sends the events to the stream server through HTTP. The
former is useful when you want the producer of the events to act as a
server for its own events. The latter is useful for situations in
which sensors (or event producers of any kind) are scattered in the
network and separate from the stream server that serves the events
they produce.

When publishing events through a remote stream server, the producer of
the events needs to know the URI of the stream the events are to be
published to. The special path component `/publish` must be appended to
the URI of the stream. For example::

    http://example.com:9000/stream1/publish

The `EventPublisher` and `SynchronousEventPublisher` append
automatically `/publish` to the URI they receive if it does not
contain it.


Publishing events from the stream server
........................................

The following example sets up a stream server that serves two streams,
and publishes periodical events on them:

    .. include:: ../examples/server.py
       :literal:

The key aspects to take into account in the previous example are that:

- You must program asynchronously, because the server is asynchronous.

- You have to use a `ztreamy.LocalEventPublisher` object for
  publishing events in a stream. Events are published with its
  `publish` method or its `publish_events` method. The difference
  between them is just that `publish` receives a single event object,
  whereas `publish_events` receives a list of events.


Publishing events through a remote stream server asynchronously
...............................................................

The following example publishes periodic events using the asynchronous
API:

    .. include:: ../examples/publisher_async.py
       :literal:

The program creates an `EventPublisher` object and publishes a new
event every 10 seconds, by using its `publish` method. Note that the
program needs to block on Tornado's *ioloop* at the end, in order to
work. Because of that, the timer of *ioloop* is used for scheduling
the creation of events.


Publishing events through a remote stream server synchronously
..............................................................

The following example publishes periodic events using the synchronous
API:

    .. include:: ../examples/publisher_sync.py
       :literal:

The main difference with the previous example is that now the program
does not block on the *ioloop*, but uses sleep to control the rate at
which the events are published.


Event objects
-------------

What all the events have in common
..................................

RDF events
..........

Creating custom event classes
.............................


Selecting specific events (filtering)
-------------------------------------

