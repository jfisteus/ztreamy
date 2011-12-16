import pycurl
import cjson as json
import rdflib
import tornado
import traceback

# from geopy import geocoders
from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPResponse
from tornado.simple_httpclient import SimpleAsyncHTTPClient

import streamsem
from streamsem import events
from streamsem import rdfevents
from streamsem import client

class TwitterStreamSensor():
   
    def __init__(self, publisher, app_id = "TwitterSensor", source_id = "TwitterSensor0"):
    	self.STREAM_URL = "https://stream.twitter.com/1/statuses/sample.json"
	self.USER = "gimi_uc3m"
	self.PASS = "qabasabslc10"
	self.NS = Namespace("http://webtlab.it.uc3m.es/")
	self.DC = Namespace("http://purl.org/dc/elements/1.1/")
	self.publisher = publisher
	self.app_id = app_id
	self.source_id = source_id
	# self.geo = geocoders.GeoNames()

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
	if "coordinates" in tweet_dict:
		if str(tweet_dict["coordinates"]) != "None":
			(longitude,latitude) = tweet_dict["coordinates"]["coordinates"]
			coordinates = "(%s,%s)" % (longitude,latitude)
			graph.add( ( tweet_id, self.NS["position"], Literal(coordinates) ) )

	return graph

    def decode(self, tweet):
	tweet_dict = json.decode(tweet)
	# Forget tweets that do not contain id or timestamp
	if "id" in tweet_dict and "created_at" in tweet_dict:
	   return self.toN3(tweet_dict)

    def start (self):
	conn = pycurl.Curl()
	conn.setopt(pycurl.USERPWD, "%s:%s" % (self.USER, self.PASS))
	conn.setopt(pycurl.URL, self.STREAM_URL)
	conn.setopt(pycurl.WRITEFUNCTION, self.on_receive)
	conn.perform()

    def start_async(self):
        http_client = SimpleAsyncHTTPClient()
        req = HTTPRequest(self.STREAM_URL,
                          streaming_callback=self.on_receive,
                          auth_username=self.USER,
                          auth_password=self.PASS,
                          request_timeout=0, connect_timeout=0)
        http_client.fetch(req, self.on_receive)
        print "Launched Twitter request"

    def on_receive(self, data):
        if isinstance(data, HTTPResponse):
            data = data.body
	try:
		graph = self.decode(data)
		self.publish(graph)	
	except:
            # Forget the tweets that produce processing errors	
            #traceback.print_exc()
            return

    def publish(self, graph):	
	event = rdfevents.RDFEvent(self.source_id, 'text/n3', graph)
	print event
	self.publisher.publish(event)

def main():
    publisher = client.EventPublisher("http://localhost:9001/events/publish")
    enc = TwitterStreamSensor(publisher,"AppID","SrcID")
    enc.start_async()
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()


