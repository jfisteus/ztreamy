# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2015 Norberto Fernandez Garcia
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

""" This application follows a user on Twitter pooling periodically
    his/her timeline and generating events with each tweet
"""

from __future__ import print_function

import argparse
import tornado.ioloop
import tornado.options

from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef

from ztreamy import rdfevents
from ztreamy import client
from ztreamy import logger
from ztreamy.tools import utils
from ztreamy.examples.utils.twitterRESTclient import TwitterRESTclient


class TwitterFollowerBot():
    """ A bot that follows a user on Twitter pooling periodically
        his/her timeline and generating events with each tweet
    """
    
    def __init__(self, publisher, user, time, source_id, app_id="TwitterFollowerBot"):
        
        self.NS = Namespace("http://webtlab.it.uc3m.es/")
        self.DC = Namespace("http://purl.org/dc/elements/1.1/")
        self.publisher = publisher
        self.app_id = app_id
        self.source_id = source_id
        self.io_loop = tornado.ioloop.IOLoop.instance()
        self.num_events_created = 0
        self.scheduler = utils.get_scheduler("const[" + str(time) + "]")
        self.client = TwitterRESTclient(user)
        self.last_id = 0

    def toN3(self, tweet):
        
        graph = Graph()
        graph.bind("webtlab", "http://webtlab.it.uc3m.es/")
        graph.bind("dc", "http://purl.org/dc/elements/1.1/")
        # Set the triple ID as the tweet ID
        tweet_id = URIRef("http://webtlab.it.uc3m.es/_" + str(tweet.id))
        # Add the creation timestamp
        graph.add((tweet_id, self.DC["created"], Literal(tweet.created_at)))
        # Get the text of the tweet
        graph.add((tweet_id, self.NS["content"], Literal(tweet.text)))
        return graph

    def schedule_next_event(self):
        self.io_loop.add_timeout(self.scheduler.next(), self.publish_event)

    def publish_event(self):
        self.schedule_next_event()
        tweets = self.client.get_tweets(self.last_id)
        if len(tweets) > 0:
          print("Generating {} events...".format(len(tweets)))
          for tweet in tweets:
            n3rdf = self.toN3(tweet)
            event = rdfevents.RDFEvent(self.source_id, 'text/n3', n3rdf, application_id=self.app_id)
            print(event)
            self.publisher.publish(event)
            self.last_id = tweet.id
        else:
          print("No new events to be generated...")

    def start(self):
        self.schedule_next_event()
        self.io_loop.start()

    def finish(self):
        self.publisher.close()
        tornado.ioloop.IOLoop.instance().stop()


def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--time", dest="time", type=int, default=10,
                  help="time between two sucessive queries to Twitter API (in seconds, e.g. 10)")
    parser.add_argument("-u", "--user", dest="user", required=True, 
                  help="print name of the Twitter user to follow (e.g. nordez)")
    parser.add_argument("-a", "--appid", dest="appid", default="TwitterFollowerBot",
                  help="application identifier (added to generated events)")
    parser.add_argument("-s", "--sourceid", dest="sourceid", required=True, 
                  help="source identifier (added to generated events)")
    parser.add_argument("-o", "--output", dest="output", required=True,
                  help="URL for output stream where events are published (e.g. http://localhost:9001/events/publish)")
    options = parser.parse_args()

    publisher = client.EventPublisher(options.output)
    bot = TwitterFollowerBot(publisher, options.user, options.time, options.sourceid, options.appid)
    
    try:
        bot.start()
    except KeyboardInterrupt:
        bot.finish()
        pass
    finally:
        logger.logger.close()

if __name__ == "__main__":
    main()
