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

""" A sensor that converts tweets obtained from the Twitter streaming API into semantic events
    publishing those events into an output stream
"""

from __future__ import print_function

import sys
import cjson as json
import tornado
import argparse
import traceback

from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef
from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPResponse

from ztreamy import rdfevents
from ztreamy import client

class TwitterStreamSensor():
    """ A sensor that converts tweets obtained from the Twitter streaming API into semantic events
    """
    
    def __init__(self, publisher, twitterUsr, twitterPasswd, source_id, application_id = "TwitterSensor", only_geo = False):

        self.STREAM_URL = "https://stream.twitter.com/1/statuses/sample.json"
        self.NS = Namespace("http://webtlab.it.uc3m.es/")
        self.DC = Namespace("http://purl.org/dc/elements/1.1/")
	self.GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
        self.USER = twitterUsr
        self.PASS = twitterPasswd
        self.publisher = publisher
        self.app_id = application_id
        self.source_id = source_id
        self.only_geo = only_geo
        self.counter = 0
        
    def toN3(self, tweet_dict):

        graph = Graph()
        graph.bind("webtlab", "http://webtlab.it.uc3m.es/")
        graph.bind("dc", "http://purl.org/dc/elements/1.1/")
        graph.bind("wgs84", "http://www.w3.org/2003/01/geo/wgs84_pos#")

        # Set the triple ID as the tweet ID
        tweet_id = URIRef("http://webtlab.it.uc3m.es/_" + str(tweet_dict["id"]))

        # Add the creation timestamp as a date
        graph.add((tweet_id, self.DC["date"],
                   Literal(tweet_dict["created_at"]) ))

        # Get the id and screen name of the tweet author
        if "user" in tweet_dict:
            user = tweet_dict["user"]
            # if "id" in user:
            #    graph.add((tweet_id, self.DC["creator"],
            #               Literal(str(user["id"]))))
            if "screen_name" in user:
                graph.add((tweet_id, self.DC["creator"],
                           Literal("@" + user["screen_name"]) ))

        # Get the text of the tweet
        if "text" in tweet_dict:
            graph.add((tweet_id, self.NS["content"],
                       Literal(tweet_dict["text"])))

        # Look for entities (urls, mentions, hashtags)
        if "entities" in tweet_dict:
            entities = tweet_dict["entities"]
            if "urls" in entities:
                for url in entities["urls"]:
                    graph.add((tweet_id, self.NS["url"],
                               Literal(url["expanded_url"])))
            if "hashtags" in entities:
                for hashtag in entities["hashtags"]:
                    graph.add((tweet_id, self.NS["hashtag"],
                               Literal("#" + hashtag["text"])))
            if "user_mentions" in entities:
                for mention in entities["user_mentions"]:
                    graph.add((tweet_id, self.NS["mention"],
                               Literal("@" + mention["screen_name"])))

        # Look for the number of retweets
        if "retweet_count" in tweet_dict:
            if tweet_dict["retweet_count"] > 0:
                graph.add((tweet_id, self.NS["retweets"],
                           Literal(str(tweet_dict["retweet_count"]))))
        
	# Look for geographic information
        if "coordinates" in tweet_dict:
            if str(tweet_dict["coordinates"]) != "None":
                longitude, latitude = tweet_dict["coordinates"]["coordinates"]
                graph.add((tweet_id, self.GEO["long"], Literal(str(longitude))))
                graph.add((tweet_id, self.GEO["lat"], Literal(str(latitude))))

        return graph


    def decode(self, tweet):

        tweet_dict = json.decode(tweet)
        # Ignore tweets that do not contain id or timestamp
        # (For instance, filters out delete notifications)
        if "id" in tweet_dict and "created_at" in tweet_dict:
            return self.toN3(tweet_dict)


    def start_async(self):
        http_client = AsyncHTTPClient()
        req = HTTPRequest(self.STREAM_URL,
                          streaming_callback=self.on_receive,
                          auth_username=self.USER,
                          auth_password=self.PASS,
                          request_timeout=0, connect_timeout=0)
        http_client.fetch(req, self.on_receive)
        print("Launched Twitter request")


    def on_receive(self, data):
        
        if isinstance(data, HTTPResponse):
            data = data.body        
        try:
            graph = self.decode(data)
            if graph != None:
                if self.only_geo:
                    # Publish only events with geographic information
                    if (self.GEO["long"] in graph.predicates()
                        and self.GEO["lat"] in graph.predicates()):
                           self.publish(graph)
                else:
                    self.publish(graph)
        except:
            # Ignore the tweets that produce processing errors
            traceback.print_exc()
            return


    def publish(self, graph):
        event = rdfevents.RDFEvent(self.source_id, 'text/n3', graph, application_id = self.app_id)
        ## print(event)
        self.publisher.publish(event)
        self.counter += 1
        ## print("*** {} events published\n".format(self.counter))
        

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-g", "--geo", dest="geo", action='store_const', const=True, default=False,
                  help="a boolean flag indicating whether all tweets, or only those that contain location information should be published")
    parser.add_argument("-u", "--user", dest="user", required=True, 
                  help="a Twitter account login")    
    parser.add_argument("-p", "--passwd", dest="passwd", required=True, 
                  help="a Twitter account password")        
    parser.add_argument("-a", "--appid", dest="appid", default="TwitterSensor",
                  help="application identifier (added to generated events)")
    parser.add_argument("-s", "--sourceid", dest="sourceid", required=True, 
                  help="source identifier (added to generated events)")
    parser.add_argument("-o", "--output", dest="output", required=True,
                  help="URL for output stream where events are published (e.g. http://localhost:9001/events/publish)")

    options = parser.parse_args()
    publisher = client.EventPublisher(options.output)
    enc = TwitterStreamSensor(publisher, options.user, options.passwd, options.sourceid, options.appid, options.geo)
    enc.start_async()
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()


