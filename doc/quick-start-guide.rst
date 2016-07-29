A quick start guide to Ztreamy
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

This guide describes how to install Ztreamy in Linux systems. It
should work also in Windows, but we have not yet tested it.

In order to install Ztreamy, you need an installation of Python 2.7.
Python 3 is not backwards compatible and is not supported yet.
Python is installed by default in many Linux distributions.
Therefore, its version 2.7 is probably already installed in your system.

You can install Ztreamy globally in your Linux installation, or in a
local directory, using *virtualenv*. The latter is probably better if
you just want to try Ztreamy, because this way it can be safely
removed just by removing the *virtualenv* directory.

First, install the *virtualenv* package using the package manager of
your Linux distribution. For example, in Debian and Ubuntu::

    sudo apt-get install python-virtualenv

Ztreamy requires the Python header files
and the *libcurl* with its header files to be
installed in your system. In Debian and Ubuntu they are packaged as
`python-dev`, `libcurl3` and `libcurl4-openssl-dev` respectively::

    sudo apt-get install python-dev libssl-dev curl libcurl3 libcurl4-openssl-dev

Now, somewhere in your user account, create the *virtualenv* directory
in which Ztreamy will be installed. For example, if you call the
directory `ztreamy-venv`::

    virtualenv ztreamy-venv
    source ztreamy-venv/bin/activate

The first command creates the virtual environment and installs Python
inside it. The second command makes this virtual environment the
default in the current terminal. The second command has to be executed
every time a new terminal is opened for using Ztreamy.

Finally, you can install Ztreamy from the PyPI software repository::

    pip install ztreamy


Upgrading Ztreamy
-----------------

If you already have an installation of Ztreamy,
*pip* can also upgrade it to the latest release::

    source ztreamy-venv/bin/activate
    pip install --upgrade ztreamy



Trying the installation for the first time
------------------------------------------

In order to try your installation of Ztreamy,
you'll create a new stream,
run a client to consume it
and run a source that publishes data through the stream.

First, remember that,
if you have installed Ztreamy inside a *virtualenv* environment,
it must be activated for every terminal
in which you run Ztreamy commands.
Enter within every terminal the command::

    source ztreamy-venv/bin/activate

Then, run a server that publishes a stream at TCP port 9000::

    python -m ztreamy.server --port=9000

In another terminal, launch a client to listen to that stream::

    python -m ztreamy.client http://localhost:9000/events/stream

Finally, you need an event source that sends events to the server
in order to be published in the stream. Ztreamy integrates a tool that
creates events for testing purposes.
Enter in another terminal::

    python -m ztreamy.tools.event_source --distribution="exp[3]" http://localhost:9000/events/publish

It will create and send events to the server. The time between events
follows an exponential distribution with an average of 3 seconds.  You
should see the events arriving at the terminal in which the client is
running.

Alternatively to the console client,
there is an integrated JavaScript client
that allows you to watch the stream from a web browser.
Access in your browser the URL
`http://localhost:9000/events/dashboard.html
<http://localhost:9000/events/dashboard.html>`_.
The page will show the latest events in the stream
and update as soon as new events are published in the stream.

In order to stop the framework, type control-c in the terminals of the
event source and the server. The client will stop automatically when
the server closes.
