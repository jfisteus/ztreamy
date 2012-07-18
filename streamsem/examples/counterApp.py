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
import sys
import tornado
import uuid
import functools

from streamsem.client import AsyncStreamingClient
from streamsem import rdfevents
from streamsem import client

from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef


mentionsDict = {}
hashtagsDict = {}
NS = Namespace("http://webtlab.it.uc3m.es/")

def buildGraph(dict, name):
''' Utilitu function to build an RDF graph from the statistics in mentionsDict and hashtagsDict
'''    
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
''' Utility function to update the counters in mentionsDict and hashtagsDict
'''
    entries = list(event.body.subject_objects(NS[name]))
    for entry in entries:
        (subj, obj) = entry
        if not obj in dict:
            dict[obj] = 1.0
        else:
            dict[obj] += 1.0


def publish(source_id, publisher):
''' Periodically called by a timer to publish events which contain the statistics
    of mentions and hashtags
'''    
    graph = buildGraph(mentionsDict, "mention")
    if graph != None:
        event = rdfevents.RDFEvent(source_id, 'text/n3', graph)
        print event
        publisher.publish(event)

    mentionsDict.clear()

    graph = buildGraph(hashtagsDict, "hashtag")
    if graph != None:    
        event = rdfevents.RDFEvent(source_id, 'text/n3', buildGraph(hashtagsDict, "hashtag"))
        print event
        publisher.publish(event)

    hashtagsDict.clear()



def process_tweet(event):
''' For each incoming tweet event, update statistics of mentions and hashtags
'''
    process(event, mentionsDict, "mention")
    process(event, hashtagsDict, "hashtag")


def main():

    if len(sys.argv) != 5:
        print('Arguments: <Input stream URL> <Output stream URL> <Timer period [msec]> <SourceId>')
        return

    inputUrl = sys.argv[1]
    outputUrl = sys.argv[2]
    period = int(sys.argv[3])
    sourceId = sys.argv[4]
    
    # Client to listen to input tweet stream
    clnt = AsyncStreamingClient(inputUrl, event_callback=process_tweet, ioloop=tornado.ioloop.IOLoop.instance())

    # Publisher to push the generated events 
    publisher = client.EventPublisher(outputUrl)
    
    # Scheduling stats event publishing
    callback = functools.partial(publish, sourceId, publisher)
    scheduler = tornado.ioloop.PeriodicCallback(callback, period)
    scheduler.start()

    clnt.start(loop=True)

    

if __name__ == "__main__":
    main()
