import tornado.ioloop
import tornado.options
import logging

from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef

import streamsem
from streamsem import events
from streamsem import rdfevents
from streamsem import client
from streamsem import logger
from streamsem.tools import utils

from twitterRESTclient import TwitterRESTclient

class TwitterFollowerBot():

    def __init__(self, publisher, user, time = 10, app_id = "TwitterFollowerBot", source_id = "TwitterFollower0"):

	self.NS = Namespace("http://webtlab.it.uc3m.es/")
	self.DC = Namespace("http://purl.org/dc/elements/1.1/")
	self.publisher = publisher
	self.app_id = app_id
	self.source_id = source_id
	self.io_loop = tornado.ioloop.IOLoop.instance()
	self.num_events_created = 0
	self.scheduler = utils.get_scheduler("const[" + str(time) + "]")
	self.client = TwitterRESTclient(user)


    def toN3(self, tweet):

	graph = Graph()

	graph.bind("webtlab", "http://webtlab.it.uc3m.es/")
	graph.bind("dc", "http://purl.org/dc/elements/1.1/")

	# Set the triple ID as the tweet ID
	tweet_id = URIRef("_" + str(tweet.id))

	# Add the creation timestamp
	graph.add( ( tweet_id, self.DC["created"], Literal(tweet.created_at) ))

	# Get the text of the tweet
	graph.add( ( tweet_id, self.NS["content"], Literal(tweet.text) ))

	return graph

	
    def schedule_next_event(self):
        self.io_loop.add_timeout(self.scheduler.next(), self.publish_event)


    def publish_event(self):        
	
	self.schedule_next_event()
	tweets = self.client.get_tweets()
	for tweet in tweets:
	   event = self.toN3(tweet)
	   self.publisher.publish(event)  				
        

    def start(self):    	
       	self.schedule_next_event()
	self.io_loop.start()


    def finish(self):
        self.publisher.close()
        tornado.ioloop.IOLoop.instance().stop()


def main():

    publisher = client.EventPublisher("http://localhost:9001/events/publish")    
    bot = TwitterFollowerBot(publisher, "nordez", 10, "AppID","SrcID")

    try:
        bot.start()
    except KeyboardInterrupt:
	bot.finish()
        pass
    finally:
        logger.logger.close()

if __name__ == "__main__":
    main()
