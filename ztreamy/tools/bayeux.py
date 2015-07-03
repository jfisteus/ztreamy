# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2015 Jesus Arias Fisteus
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.
#

"""A simple asynchronous client that sends events to a Bayeux server.

It is used only for testing purposes with the Faye server.

"""
import json

from ztreamy import EventPublisher

mimetype_json = 'application/json'


class BayeuxEventPublisher(EventPublisher):
    """Publishes events by sending them to a Bayeux server. Asynchronous.

    Uses an asynchronous HTTP client, but does not manage an ioloop
    itself. The ioloop must be run by the calling code.

    """
    def __init__(self, server_url, channel, io_loop=None):
        """Creates a new 'BayeuxEventPublisher' object.

        Events are sent in separate HTTP requests to the server given
        by 'server_url'.

        """
        super(BayeuxEventPublisher, self).__init__(server_url, io_loop=io_loop)
        self.channel = channel
        self.headers = {'Content-Type': mimetype_json}

    def publish_events(self, events, callback=None):
        """Publishes a list of events.

        The events in the list 'events' are sent to the server in a
        new HTTP request. If a 'callback' is given, it will be called
        when the response is received from the server. The callback
        receives a tornado.httpclient.HTTPResponse parameter.

        """
        for event in events:
            message = {'channel': self.channel}
            message['data'] = event.as_dictionary()
            body = json.dumps(message)
            self._send_request(body, callback=callback)
