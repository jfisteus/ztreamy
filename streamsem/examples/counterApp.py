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

from streamsem.client import AsyncStreamingClient
from streamsem import rdfevents
from streamsem import client

from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef


mentionsDict = {}
hashtagsDict = {}


def publish():
    print "********************************* Publicando..."

def process_tweet(event):

    print "Procesando..."
    NS = Namespace("http://webtlab.it.uc3m.es/")

    # Find the hashtags and mentions
    # Append the data to in-memory storage
    mentions = list(event.body.subject_objects(NS["mention"]))
    for mention in mentions:
        (subj, obj) = mention
        if not obj in mentionsDict:
            mentionsDict[obj] = 0.0
        else:
            mentionsDict[obj] += 1.0

    hashtags = list(event.body.subject_objects(NS["hashtag"]))
    for hashtag in hashtags:
        (subj, obj) = hashtag
        if not obj in hashtagsDict:
            hashtagsDict[obj] = 0.0
        else:
            hashtagsDict[obj] += 1.0



#
# This client listens to a stream of geolocated tweets
# counts the number of mentions to users and hastags
# and publishes periodically the results to an output
# event stream
#
def main():

    if len(sys.argv) != 4:
        print('Arguments: <Input stream URL> <Output stream URL> <Flush timer>')
        return

    inputUrl = sys.argv[1]
    outputUrl = sys.argv[2]
    flushTimer = int(sys.argv[3])

    timer = 10000

    
    # Client to listen to input tweet stream
    clnt = AsyncStreamingClient(inputUrl, event_callback=process_tweet, ioloop=tornado.ioloop.IOLoop.instance())

    # Publisher to push the generated events 
    publisher = client.EventPublisher(outputUrl)
    
    # 
    scheduler = tornado.ioloop.PeriodicCallback(publish, timer)
    scheduler.start()

    clnt.start(loop=True)

    

if __name__ == "__main__":
    main()
