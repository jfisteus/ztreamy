import pycurl
import cjson as json
import rdflib

from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef

import streamsem
from streamsem import events
from streamsem import rdfevents
from streamsem import client

class TwitterRDFEncoder():
   
    def __init__(self, url, app_id = "TwitterSensor", source_id = "TwitterSensor0"):
    	self.STREAM_URL = "https://stream.twitter.com/1/statuses/sample.json"
	self.USER = "gimi_uc3m"
	self.PASS = "qabasabslc10"
	self.NS = Namespace("http://webtlab.it.uc3m.es/")
	self.DC = Namespace("http://purl.org/dc/elements/1.1/")
	self.url = url
	self.app_id = app_id
	self.source_id = source_id

    def toN3(self, tweet_dict):

	graph = Graph()

	graph.bind("webtlab", "http://webtlab.it.uc3m.es/")
	graph.bind("dc", "http://purl.org/dc/elements/1.1/")

	# Set the triple ID as the tweet ID
	tweet_id = URIRef("_" + str(tweet_dict["id"]))

	# Add the creation timestamp
	graph.add( ( tweet_id, self.DC["created"], Literal(tweet_dict["created_at"]) ))

	# Get the text of the tweet
	if "text" in tweet_dict:
		graph.add( ( tweet_id, self.NS["content"], Literal(tweet_dict["text"]) ))

	# Look for entities (urls, mentions, hashtags)
	if "entities" in tweet_dict:
		entities = tweet_dict["entities"]		
		if "urls" in entities:
			for url in entities["urls"]:			
				graph.add( ( tweet_id, self.NS["url"], Literal(url["expanded_url"]) ))
		if "hashtags" in entities:
			for hashtag in entities["hashtags"]:
				graph.add( ( tweet_id, self.NS["hashtag"], Literal("#" + hashtag["text"]) ))
		if "user_mentions" in entities:
			for mention in entities["user_mentions"]:
				graph.add( ( tweet_id, self.NS["mention"], Literal("@" + mention["screen_name"]) ))
	
	# Look for the number of retweets
	if "retweet_count" in tweet_dict:
		if tweet_dict["retweet_count"] > 0:
			graph.add( ( tweet_id, self.NS["retweets"], Literal(str(tweet_dict["retweet_count"])) ))

	# Look for geographic information
	if "geo" in tweet_dict:
		if str(tweet_dict["geo"]) != "None":
			graph.add( ( tweet_id, self.NS["position"], Literal(str(tweet_dict["geo"])) ))

	print graph.serialize(format='n3')
	self.publish(tweet_id, graph)

    def decode(self, tweet):
	tweet_dict = json.decode(tweet)
	# Forget tweets that do not contain id or timestamp
	if "id" in tweet_dict and "created_at" in tweet_dict:
	   self.toN3(tweet_dict)

    def start (self):
	conn = pycurl.Curl()
	conn.setopt(pycurl.USERPWD, "%s:%s" % (self.USER, self.PASS))
	conn.setopt(pycurl.URL, self.STREAM_URL)
	conn.setopt(pycurl.WRITEFUNCTION, self.on_receive)
	conn.perform()

    def on_receive(self, data):
	self.decode(data)

    def publish(self, event_id, graph):
	p = client.EventPublisher(self.url)
	event = events.Event(self.source_id, 'n3', event_id, self.app_id)
	#self, source_id, syntax, body, event_id=None,
        #         application_id=None, aggregator_id=[], event_type=None,
        #         timestamp=None
	p.publish(event)

def main():
    enc = TwitterRDFEncoder("http://localhost:9001/events/publish","AppID","SrcID")
    enc.start()

if __name__ == "__main__":
    main()


