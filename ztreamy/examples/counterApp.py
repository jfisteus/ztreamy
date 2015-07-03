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

""" This application listens to a stream of tweets (produced for instance 
    with twitterSensor.py) counts the number of times each user mention or
    hashtag is included in tweets and, periodically, publishes to a different 
    stream new events including the statistics gathered.
"""

from __future__ import print_function

import sys
import tornado
import uuid
import functools
import argparse

from ztreamy.client import AsyncStreamingClient
from ztreamy import rdfevents
from ztreamy import client

from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef


mentionsDict = {}
hashtagsDict = {}
NS = Namespace("http://webtlab.it.uc3m.es/")

def buildGraph(dict, name):
    """ Utility function to build an RDF graph from the statistics in mentionsDict and hashtagsDict
    """    

    graph = Graph()
    graph.bind("webtlab", "http://webtlab.it.uc3m.es/")
    
    for entry in dict.keys():
        # Random UUID as identifier
        id = URIRef("http://webtlab.it.uc3m.es/_" + str(uuid.uuid4()))
        graph.add((id, NS[name], Literal(entry)))
        graph.add((id, NS["counter"], Literal(str(dict[entry]))))

    if len(graph) > 0:
        return graph
    else:
        return None


def process(event, dict, name):
    """ Utility function to update the counters in mentionsDict and hashtagsDict
    """

    entries = list(event.body.subject_objects(NS[name]))
    for entry in entries:
        (subj, obj) = entry
        if not obj in dict:
            dict[obj] = 1.0
        else:
            dict[obj] += 1.0


def process_tweet(event):
    """ For each incoming tweet event, update statistics of mentions and hashtags
    """

    process(event, mentionsDict, "mention")
    process(event, hashtagsDict, "hashtag")

    
def publish(app_id, source_id, publisher):
    """ Periodically called by a timer to publish events which contain the statistics of mentions and hashtags
    """
    
    graph = buildGraph(mentionsDict, "mention")
    if graph != None:
        event = rdfevents.RDFEvent(source_id, 'text/n3', graph, application_id = app_id)
        print(event)
        publisher.publish(event)

    mentionsDict.clear()

    graph = buildGraph(hashtagsDict, "hashtag")
    if graph != None:    
        event = rdfevents.RDFEvent(source_id, 'text/n3', graph, application_id = app_id)
        print(event)
        publisher.publish(event)

    hashtagsDict.clear()



def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--time", dest="time", type=int, default=30,
                  help="period to generate new events with statistics (in seconds, defaults to 30)")
    parser.add_argument("-a", "--appid", dest="appid", default="StatsGenerator",
                  help="application identifier (added to generated events)")
    parser.add_argument("-s", "--sourceid", dest="sourceid", required=True, 
                  help="source identifier (added to generated events)")
    parser.add_argument("-i", "--input", dest="input", required=True,
                  help="URL for input stream where events are read (e.g. http://localhost:9001/events/stream)")
    parser.add_argument("-o", "--output", dest="output", required=True,
                  help="URL for output stream where events are published (e.g. http://localhost:9002/events/publish)")

    options = parser.parse_args()

    inputUrl = options.input
    outputUrl = options.output
    period = options.time*1000
    appId = options.appid
    sourceId = options.sourceid
    
    # Client to listen to input tweet stream
    clnt = AsyncStreamingClient(inputUrl, event_callback=process_tweet, ioloop=tornado.ioloop.IOLoop.instance())

    # Publisher to push the generated events 
    publisher = client.EventPublisher(outputUrl)
    
    # Scheduling stats event publishing
    callback = functools.partial(publish, appId, sourceId, publisher)
    scheduler = tornado.ioloop.PeriodicCallback(callback, period)
    scheduler.start()

    clnt.start(loop=True)

    

if __name__ == "__main__":
    main()
