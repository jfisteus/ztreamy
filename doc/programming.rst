
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
....................................................

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


Running the examples of this guide
..................................

The examples of this guide are distributed inside the `examples`
directory of the source code of Ztreamy. Assuming you have `installed
Ztreamy <quick-start-guide.html>`_, you can run them from a console.

For running consumers, you need to have the example server running::

    $ python server.py

Then, in another console, run the client you want to try::

    $ python client_async.py

If you want to try the event producer, you need to run the server and
a client as shown above, and then, in another console::

    $ python publisher_async.py


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

- Long-polling requests: every time the client wants more data, it
  issues a new HTTP request. The server returns the events that are
  new since the previous request of the client and finishes the
  request. If there are no new events, the server keeps the request on
  hold until at least one event is available.

The access mode is communicated to the server by appending to the path
of the URI one of the following components:

- `/compressed`: long-lived requests with *deflate* compression (ZLib).

- `/stream`: long-lived requests with optional compression.
  Set the *Accept-Encoding* header to *deflate* in order to
  receive the data with *deflate* compression (ZLib).

- `/long-polling`: long-polling requests with no compression.


Developing a consumer asynchronously
....................................

The following program connects to two streams and prints the events
that come from them:

.. include:: ../examples/python/consumer_async.py
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
be the preferred option, due to the amount of traffic it saves.

Be aware that the HTTP client library of Tornado
internally limits the maximum number of simultaneously active HTTP requests
on the same IOLoop.
The default in Tornado (with the CURL client, which is the one Ztreamy uses)
is currently set to 10.
If you need to subscribe to more than 10 simultaneous streams,
you'll need to increase that maximum value.
This can be done with a simple function call,
as shown in the following example,
which raises the maximum to 200 simultaneous requests::

    import ztreamy.client

    ztreamy.client.configure_max_clients(200)


Developing a consumer synchronously
...................................

The following program connects to a stream with the synchronous API
and prints the events that come from it:

.. include:: ../examples/python/consumer_sync.py
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
long polling requests mode: the stream name in the path must be
followed by `/long-polling`, like in the example.


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

.. include:: ../examples/python/server.py
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

.. include:: ../examples/python/publisher_async.py
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

.. include:: ../examples/python/publisher_sync.py
   :literal:

The main difference with the previous example is that now the program
does not block on the *ioloop*, but uses sleep to control the rate at
which the events are published.


Event objects
-------------

Ztreamy serializes events as a series of headers and a body. A header
is similar to an HTTP header. It contains a name and a value. The body
contains the main data of the event. There is no assumption on the
kind of data that the body stores. This is an example serialization of
an event::

    Event-Id: 1100254f-f4ba-49aa-8c47-605e3110169e
    Source-Id: 83a4c888-c395-4bb7-a635-c5b864d6bd06
    Syntax: text/n3
    Application-Id: identi.ca dataset
    Timestamp: 2012-10-25T13:31:24+02:00
    Body-Length: 843
    
    @prefix dc: <http://purl.org/dc/elements/1.1/> .
    @prefix foaf: <http://xmlns.com/foaf/0.1/> .
    @prefix geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> .
    @prefix webtlab: <http://webtlab.it.uc3m.es/ns/> .
    
    <http://identi.ca/notice/97535534> dc:creator "http://identi.ca/user/94360";
        dc:date "2012-10-25T11:28:51+00:00";
        webtlab:content "Completed registrations for #wmbangalore !Wikimedia
                         DevCamp Banglalore: 2430 applications, 130 invitations
                         sent http://is.gd/FtXMhT";
        webtlab:conversation "http://identi.ca/conversation/96703048";
        webtlab:hashtag "wmbangalore";
        webtlab:location [ a geo:Place;
                geo:lat "13.018",
                geo:long "77.568" ] .
    
    "http://identi.ca/user/94360" foaf:based_near [ a geo:Place;
                geo:lat "52.392";
                geo:long "4.899" ];
        foaf:name "S....... M......" .

The serialization of an event object consists of a series of headers,
an empty line and a body. Analogously to HTTP, the end-of-line
delimiter for the header lines and the empty line must be the two-byte
sequence CRLF.  However, Ztreamy imposes no restrictions regarding
end-of-line delimiters in the body of the event, which should adhere
the specifications for the specific data-type (e.g. for N3, RDF/XML
and JSON-LD both LF and CRLF delimiters are allowed by their
specifications.)

Ztreamy provides an API for representing events as objects, and for
serializing and deserializing them. The `Event` class is the base
class for all the events. Classes for specific types of events, such
as `RDFEvent`, which is used events whose body is RDF, subclass
`Event`.


Creating event objects
......................

You can create an event directly using the `Event` class or using one
of its subclass. This is an example of a generic event::

    import ztreamy
    source_id = ztreamy.random_id()
    event = ztreamy.Event(source_id, 'text/plain',  'This is a new event')

If there is an appropriate class for representing a type of event,
events should be created with the constructor of that class (see the
example for RDF events below).


Accessing event objects
.......................

In order to access the contents of an event object, you can use its
attributes: `event_id`, `source_id`, `syntax`, `application_id`,
`aggregator id`, `event_type`, `timestamp`, `extra_headers` (a
dictionary with the application-specific headers) and `body`.

All the attributes above can also be accessed through the dictionary
that the method `as_dictionay` returns::

    dictionary = event.as_dictionary()
    print dictionary['Source-Id']

In addition, you can obtain a textual representation of its body with
the method `serialize_body`::

    print event.serialize_body()


RDF events
..........

Ztreamy uses internally the `rdflib library
<https://github.com/RDFLib>`_ to work with RDF data.  The events whose
body is represented as RDF are represented as objects of the
`RDFEvent` class. This is an example of an RDF event, in which an RDF
graph is used for the body of the event::

    import ztreamy
    from rdflib import Graph, Namespace, Literal
    source_id = ztreamy.random_id()
    graph = Graph()
    ns_example = Namespace('http://example.com/ns/')
    graph.add((ns_example['dog'], ns_example['eats'], Literal('10')))
    event = ztreamy.RDFEvent(source_id, 'text/n3', graph)

`RDFEvent` objects return the body of the event also as an *rdflib*
`Graph` object.


Creating custom event classes
.............................

In order to create a custom event type, you must create a class that
extends from `Event`. It should have a constructor and the code for
serializing and deserializing the body of the events. The constructor
must receive the parameters `source_id, syntax, body, **kwargs` and
call the constructor of its superclass.

Then, you need to registrer in the system the MIME types it
handles. This way, when the platform finds an event of one of those
types, it automatically creates the event using the custom class.

As an example, this is the source code of the implementation of
`RDFEvent` in ztreamy:

.. include:: ../examples/python/publisher_async.py
   :literal:


JSON serialization of events
............................

Since version 0.3, events can optionally be encoded
as JSON objects.
This is quite convenient, for example,
for sending or receiving JSON-formatted events
from a JavaScript client running within a web browser.

The body can be represented as JSON or any other textual format.
All the event headers are top-level properties
in the JSON object.
However, the *Body-Length* header is not necessary,
because the body is properly delimited
by the JSON syntax.
For example, this is a JSON-serilized event
in which the event body is also a JSON object::

    {
        "Event-Id": "124e409a-6157-48a6-b2f1-32b838584dc2",
        "Source-Id": "83a4c888-c395-4bb7-a635-c5b864d6bd06",
        "Timestamp": "2015-02-04T18:44:36+01:00",
        "Syntax": "application/json",
        "Body": {
            "speed": "40.5",
            "location": "Madrid"
        }
    }

The following example shows an RDF body with JSON-LD syntax
(the event body is cropped for clarity)::

    {
        "Event-Id": "3f2fbe91-0850-4fe1-914a-79b577db200f",
        "Source-Id": "83a4c888-c395-4bb7-a635-c5b864d6bd06",
        "Timestamp": "2015-02-04T18:57:47+01:00",
        "Syntax": "application/ld+json",
        "Body": [{"http://www.w3.org/2003/01/geo/wgs84_pos#long": (...)
    }

For any body syntax different to JSON or JSON-LD,
the body is specified just as a string.
For example, a *text/plain* body would be represented as::

    {
        "Event-Id": "9b80f271-f3dc-4968-922b-12e1a6c60464"
        "Source-Id": "83a4c888-c395-4bb7-a635-c5b864d6bd06",
        "Timestamp": "2015-02-04T19:09:12+01:00",
        "Syntax": "text/plain",
        "Body": "Call me at 5pm"
    }

You can also serialize several events together.
In order to do that,
encapsulate each event as an object within a JSON array::

    [
        {
            "Event-Id": "3f2fbe91-0850-4fe1-914a-79b577db200f",
            "Source-Id": "83a4c888-c395-4bb7-a635-c5b864d6bd06",
            "Timestamp": "2015-02-04T18:57:47+01:00",
            "Syntax": "application/ld+json",
            "Body": [{"http://www.w3.org/2003/01/geo/wgs84_pos#long": (...)
        },
        {
            "Event-Id": "9b80f271-f3dc-4968-922b-12e1a6c60464"
            "Source-Id": "83a4c888-c395-4bb7-a635-c5b864d6bd06",
            "Timestamp": "2015-02-04T19:09:12+01:00",
            "Syntax": "text/plain",
            "Body": "Call me at 5pm"
        }
    ]


When you send a JSON object to a Ztreamy server,
the *Content-Type* header of the HTTP request
must be set to *application/json*.

When you consume events from a stream,
you can request events to be serialized with JSON
only in long-polling requests.
For long-lived requests,
the Ztreamy event serialization format is the only one
permitted right now.
In order to get JSON events
in for long-polling request,
set the *Accept* header to *application/json*.


Selecting specific events (filtering)
-------------------------------------

The `ztreamy.filters` module provides a base class for filtering
events, called `Filter`, and several subclasses that implement some
built-in filter. If you need to select just a subset of the events,
you can use one of those built-in filters or program your own filter
by subclassing the `Filter` class.

The built-in filters currently available in Ztreamy are:

- `SourceFilter`: selects the events that match one of the given
  source identifiers.

- `ApplicationFilter`: selects the events that match one of the given
  application identifiers.

- `VocabularyFilter`: selects the RDF events that contain URIs for
  which one of the given URI prefixes match.

- `SimpleTripleFilter`: select the RDF events whose bodies contain
  triples that match the given triple pattern, given by subject,
  predicate and object. Not all the three components need to be
  specified.

- `SPARQLFilter`: selects the RDF events that match a given SPARQL ASK
  query.

- `TripleFilter`: selects events containing certain triple patterns,
  including boolean expressions that combine these patterns. This
  filter uses internally the `SPARQLFilter`, but receives the patterns
  with a different syntax.


Implementing a custom filter class
..................................

For implementing a custom filter class, just create a class that
extends `Filter` and implements the method `filter_event(self,
event)`. Remember to ask in the constructor of your class for a
callback function, and invoke the constructor of `Filter` with that
callback.

As an example, look how the built-in filters are programmed. For
example, this is the implementation of `SourceFilter`::

    class SourceFilter(Filter):
        def __init__(self, callback, source_id=None, source_ids=[]):
            """Creates a filter for source ids.
    
            'source_id' must be only one id, whereas 'source_ids' must be
            a list of ids. If both are present, 'source_id' is appended to
            the list of ids.
    
            """
            super(SourceFilter, self).__init__(callback)
            self.source_ids = set()
            if source_id is not None:
                self.source_ids.add(source_id)
            for source in source_ids:
                self.source_ids.add(source)
    
        def filter_event(self, event):
            if event.source_id in self.source_ids:
                self.callback(event)


Filtering events in a client
............................

Filter objects can act as event handlers. You can filter events when
using a client object by passing an instance of the filter in the
place of the event callback. This is an example::

    filter_ = ApplicationFilter(callback, application_id='ztreamy-example-a')
    client = Client(streams, event_callback=filter_, error_callback=error)

In the example, the filter receives the callback function and some
filter-specific configuration parameters (in this case, the
application id to filter). Then, it creates the client and passes the
filter object as an event callback.

This is a complete example that you can find in the examples directory
under the name of `consumer_filter.py`:

.. include:: ../examples/python/publisher_async.py
   :literal:

In order to try it, run the example server of `Publishing events from
the stream server`_ and, in another terminal, run the consumer above.
You can compare the output of this consumer with the one described at
`Developing a consumer asynchronously`_. which does not apply filters.


Filtering events in a relay
............................

You can also apply filters in the `RelayStream` class, which
implements a stream that relays the events of other streams. Pass a
filter object with the keyword parameter `filter_` to the constructor
of `RelayStream`. Only the events that match the filter will be
relayed. Note that `RelayStream` will overwrite the callback of the
filter with its own internal code.
