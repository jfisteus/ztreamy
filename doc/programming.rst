Programming applications in Ztreamy
===================================

:Author: Jesús Arias Fisteus

.. contents::


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


Developing a consumer asynchronously
....................................

The following program connects to two streams and prints the events
that come from them:

    .. include:: ../examples/consumer_async.py
       :literal:
