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

""" A sensor that monitors an Air Quality Egg (AQE) feed in COSM, converts the data to RDF events
    and publishes those events into an output stream
"""

from __future__ import print_function

import ast
import tornado
import argparse
import traceback
import httplib2
import ztreamy

from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef

from ztreamy import rdfevents
from ztreamy import client


class COSMAQESensor():
    """ A sensor that monitors Air Quality Egg (AQE) feeds in COSM
    """
    
    def __init__(self, publisher, cosmKey, feedUrl, application_id = "AQESensor"):

        self.FEED_URL = feedUrl
        self.NS = Namespace("http://webtlab.it.uc3m.es/")
        self.AQE = Namespace("http://airqualityegg.wikispaces.com/")
        self.DC = Namespace("http://purl.org/dc/elements/1.1/")
    	self.GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
        self.KEY = cosmKey
        self.http = httplib2.Http(disable_ssl_certificate_validation=True)		
        self.publisher = publisher
        self.app_id = application_id
        self.counter = 0
        

    def toN3(self, aqe_dict):

        graph = Graph()
        graph.bind("webtlab", "http://webtlab.it.uc3m.es/")
        graph.bind("cosm", "https://cosm.com/feeds/")
        graph.bind("aqe", "http://airqualityegg.wikispaces.com/")
        graph.bind("dc", "http://purl.org/dc/elements/1.1/")
        graph.bind("wgs84", "http://www.w3.org/2003/01/geo/wgs84_pos#")

        source_id = str(aqe_dict["id"])
        source_uri = URIRef("https://cosm.com/feeds/" + source_id)

        # Including basic Dublin Core metadata about the feed
        if "created" in aqe_dict:
            graph.add((source_uri, self.DC["date"],
                   Literal(aqe_dict["created"]) ))        

        if "description" in aqe_dict:
            graph.add((source_uri, self.DC["description"],
                   Literal(aqe_dict["description"]) )) 

        if "title" in aqe_dict:
            graph.add((source_uri, self.DC["title"],
                   Literal(aqe_dict["title"]) ))   

        if "title" in aqe_dict:
            graph.add((source_uri, self.DC["title"],
                   Literal(aqe_dict["title"]) ))   

        if "creator" in aqe_dict:
            graph.add((source_uri, self.DC["creator"],
                   Literal(aqe_dict["creator"]) ))   

        # Including geolocation information
        if "location" in aqe_dict:
            location = aqe_dict["location"]
            if "ele" in location:
                graph.add((source_uri, self.GEO["alt"],
                       Literal(str(location["ele"])) ))   

            if "lon" in location:
                graph.add((source_uri, self.GEO["long"],
                       Literal(str(location["lon"])) ))   
            
            if "lat" in location:
                graph.add((source_uri, self.GEO["lat"],
                       Literal(str(location["lat"])) ))   

        # Including sensor data
        datastreams = aqe_dict["datastreams"]
        for datastream in datastreams:
            datastream_id = str(datastream["id"])
            datastream_uri = URIRef("https://cosm.com/datastreams/" + datastream_id)
            graph.add((source_uri, self.NS["datastream"], datastream_uri )) 

            if "current_value" in datastream:
                graph.add((datastream_uri, self.NS["value"], Literal(str(datastream["current_value"])) ))

            if "at" in datastream:
                graph.add((datastream_uri, self.NS["timestamp"], Literal(str(datastream["at"])) ))
 
            if "unit" in datastream:
                unit = datastream["unit"]
                if "label" in unit:
                    graph.add((datastream_uri, self.NS["unit"], Literal(str(unit["label"])) ))

            if "tags" in datastream:
                tags = datastream["tags"]
                for tag in tags:
                    (name, value) = tag.split("=")
                    (prefix, suffix) = name.split(":")
                    if prefix == "aqe":
                        graph.add((datastream_uri, self.AQE[suffix], Literal(str(value)) )) 
 
        return (source_id, graph)



    def decode(self, data):
        aqe_dict = ast.literal_eval(data)
        if "id" in aqe_dict and "datastreams" in aqe_dict:
            return self.toN3(aqe_dict)


    def lookForUpdates(self):
        print("Requesting sensor data...")
        response, content = self.http.request(self.FEED_URL, headers={"X-ApiKey": self.KEY})
        if response.status != 200:
            return
        try:
            (source_id, graph) = self.decode(content)
            if source_id != None and graph != None:
                self.publish(source_id, graph)
        except:
            traceback.print_exc()
            return


    def publish(self, source_id, graph):
        event = rdfevents.RDFEvent(source_id, 'text/n3', graph, application_id = self.app_id)
        ## print(event)
        self.publisher.publish(event)
        self.counter += 1
        ## print("*** {} events published\n".format(counter))
        

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-k", "--key", dest="key", required=True, 
                  help="a COSM key")        
    parser.add_argument("-t", "--time", dest="time", type=int, default=30,
                  help="period to query COSM (in seconds, defaults to 30)")
    parser.add_argument("-a", "--appid", dest="appid", default="AQESensor",
                  help="application identifier (added to generated events)")
    parser.add_argument("-f", "--feed", dest="feed", required=True, 
                  help="the COSM URL of the JSON feed to be followed")
    parser.add_argument("-o", "--output", dest="output", required=True,
                  help="URL for output stream where events are published (e.g. http://localhost:9001/events/publish)")

    options = parser.parse_args()
    publisher = client.EventPublisher(options.output)
    enc = COSMAQESensor(publisher, options.key, options.feed, options.appid)
    sched = tornado.ioloop.PeriodicCallback(enc.lookForUpdates, options.time * 1000)
    sched.start()
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()


