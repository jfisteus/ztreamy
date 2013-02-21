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

In order to install Ztreamy, you need an installation of Python 2.6 or
Python 2.7. Python 3 is not backwards compatible and is not supported
yet. Python is installed by default in many Linux
distributions. Therefore, it is probably already installed in your
system.

You can install Ztreamy globally in your Linux installation, or in a
local directory, using *virtualenv*. The latter is probably better if
you just want to try Ztreamy, because this way it can be safely
removed just by removing the *virtualenv* directory.

First, install the *virtualenv* package using the package manager of
your Linux distribution. For example, in Debian and Ubuntu::

    sudo apt-get install python-virtualenv

Ztreamy requires the *libcurl* library and its development files to be
installed in your system. In Debian and Ubuntu they are packaged as
`libcurl3` and `libcurl-dev`::

    sudo apt-get install curl libcurl3 libcurl4-openssl-dev

Now, somewhere in your user account, create the *virtualenv* directory
in which Ztreamy will be installed. For example, if you call the
directory `venv`::

    virtualenv venv
    . venv/bin/activate

The first command creates the virtual environment and installs Python
inside it. The second command makes this virtual environment the
default in the current terminal. The second command has to be executed
every time a new terminal is opened for using Ztreamy.

Finally, you have to download Ztreamy and install it. Download the
file `ztreamy-<version>.tar.gz` (where `<version>` represents the
version number of the release of Ztreamy) from the `downloads page of
Ztreamy <http://www.it.uc3m.es/jaf/ztreamy/download/>`_. Then, install
it with `easy_install`. For example, if you downloaded
`ztreamy-0.1.1.tar.gz` you would install it with::

    easy_install ztreamy-0.1.1.tar.gz

The `easy_install` command will install Ztreamy inside the virtual
environment. In addition, it will also download and install all the
other python packages required by Ztreamy.


Trying the installation for the first time
------------------------------------------

In order to try your installation of Ztreamy, you can create a stream,
send events to it from an event source and run a client to see what is
transmitted through the stream::

    python -m ztreamy.server --port=9000

This command launches an HTTP server in the port 9000 of your computer,
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
