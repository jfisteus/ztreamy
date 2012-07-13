# streamsem: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2012 Norberto Fernandez Garcia
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
import pycurl
import cjson as json
import tornado

# from geopy import geocoders
from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef
from tornado.httpclient import HTTPRequest, HTTPResponse
from tornado.simple_httpclient import SimpleAsyncHTTPClient

from streamsem import rdfevents
from streamsem import client

class TwitterStreamSensor():

    def __init__(self, publisher, app_id = "TwitterSensor",
                 source_id = "TwitterSensor0", only_geo = False):
        self.STREAM_URL = "https://stream.twitter.com/1/statuses/sample.json"
        self.USER = "gimi_uc3m"
        self.PASS = "qabasabslc10"
        self.NS = Namespace("http://webtlab.it.uc3m.es/")
        self.DC = Namespace("http://purl.org/dc/elements/1.1/")
        self.publisher = publisher
        self.app_id = app_id
        self.source_id = source_id
        self.only_geo = only_geo
        # self.geo = geocoders.GeoNames()

    def toN3(self, tweet_dict):
        graph = Graph()
        graph.bind("webtlab", "http://webtlab.it.uc3m.es/")
        graph.bind("dc", "http://purl.org/dc/elements/1.1/")
        # Set the triple ID as the tweet ID
        tweet_id = URIRef("_" + str(tweet_dict["id"]))
        # Add the creation timestamp
        graph.add((tweet_id, self.DC["created"],
                   Literal(tweet_dict["created_at"]) ))
        # Get the id and screen name of the tweet author
        if "user" in tweet_dict:
            user = tweet_dict["user"]
            if "id" in user:
                graph.add((tweet_id, self.DC["author"],
                           Literal(str(user["id"]))))
                if "screen_name" in user:
                    graph.add((tweet_id, self.NS["userName"],
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
                graph.add((tweet_id, self.NS["longitude"], Literal(longitude)))
                graph.add((tweet_id, self.NS["latitude"], Literal(latitude)))
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
            if self.only_geo:
                if (self.NS["longitude"] in graph.predicates()
                    and self.NS["latitude"] in graph.predicates()):
                    self.publish(graph)
                else:
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
    enc = TwitterStreamSensor(publisher,"AppID","SrcID", only_geo = True)
    enc.start_async()
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()


