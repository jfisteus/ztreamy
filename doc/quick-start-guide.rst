A quick start guide to Streamsem
==================================

:Author: Jesús Arias Fisteus

.. contents::


Introduction
------------

Ztreamy is a middleware for publishing streams of
semantically-described events on the Web. It is implemented on top of
the `Tornado Web Server <http://www.tornadoweb.org/>`_.  This guide
describes how to install the software, start a simple stream and
consume its events.

Installation
------------

In order to install Ztreamy, you need an installation of Python 2.6 or
Python 2.7. Python 3 is not backwards compatible and is not supported
yet. Python can be downloaded from...



Trying the installation for the first time
------------------------------------------

In order to try your installation of Ztreamy, you can create a stream,
send events to it from an event source and run a client to see what is
transmitted through the stream::

    python -m ztreamy.server --port=9000

This command launches an HTTP server in the port 9000 of your computer
and serves an event stream from it.

In another command window, launch a client to listen to that stream::

    python -m ztreamy.client http://localhost:9000/events/stream

Finally, you need an event source that sends events to the server
in order to be published in the stream. Ztreamy integrates a tool that
creates events for testing purposes::

    python -m ztreamy.tools.event_source --distribution="exp[3]" http://localhost:9000/events/publish

It will create and send events to the server. The time between events
follows an exponential distribution with an average of 3 seconds.  You
should see the events arriving at the terminal in which the client is
running.

In order to stop the framework, type control-c in the terminals of the
event source and the server. The client will stop automatically when
the server is finished.
