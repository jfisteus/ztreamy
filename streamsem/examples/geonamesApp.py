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

from geonamesClient import GeonamesClient
from streamsem.client import SynchronousClient

from streamsem import rdfevents
from streamsem import client

from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef
#
# This client listens to a stream of geolocated tweets
# (produced for instance with twitterSensor.py)
# searches for the geonamesId of the tweet geolocation
# and in case the geonamesId is included in a list
# of identifiers to be watched, publishes the tweet in
# to another event stream
#
def main():

    if len(sys.argv) != 4:
        print('Arguments: <Twitter stream URL> <Filter [id:2521883|country:ES]> <OnlyInWatchedList [True|False]>')
        return

    GEONAMES = Namespace("http://www.geonames.org/ontology#") 
    GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
    DC = Namespace("http://purl.org/dc/elements/1.1/")

    url = sys.argv[1]
    filter = sys.argv[2].split(":")
    query = filter[0]
    datum = filter[1]
    
    if query != "id" and query != "country":
        print('Argument <Filter> should be: [id|country]:[#|countryCode]. Ex.: id:2521883, country:ES')
        return         

    onlyWatched = (sys.argv[3] == "True")

    # Client to listen to geolocated tweet stream
    clnt = SynchronousClient(url)

    # Access the geonames API
    geo = GeonamesClient(username = "dummy")

    # Case 1: filter by list of geonamesIds
    watchedIds = []
    if query == "id":
        # Expand the geonamesId to a list of relevant ids to be watched
        # watchedIds = geo.children(int(datum), godown=1)
        # Mockup for Madrid area, previously obtained by calling: children(3117735, godown=1)
        watchedIds = [6545086, 6544487, 6545080, 6947399, 6545095, 6544493, 3125239, 6544492, 6545081, 6544494, \
                           124964, 6545089, 6545079, 6545097, 3123115, 3120635, 3119589, 6545077, 6545082, 3118903, \
                           6545078, 6545084, 6544099, 6545085, 3116156, 6545090, 6545088, 3113943, 6324376, 3119198, \
                           3112772, 3112737, 6544495, 6544491, 6545087, 6545083, 3108118, 6544490, 3106970]

        print "List of geonamesIds to be watched: ",watchedIds
    
    # Case 2: filter by country code
    watchedCountry = []
    if query == "country":
        watchedCountry.append(datum)
        print "Country code to be watched ",watchedCountry


    # Publisher to push the generated events 
    publisher = client.SynchronousEventPublisher("http://localhost:9002/events/publish")

    while True:
        events = clnt.receive_events()
        for event in events:
            # Bind the GEONAMES namespace in the graph
            event.body.bind("geo", "http://www.geonames.org/ontology#")

            # Find the tweet id
            (tweet_id, obj) = list(event.body.subject_objects(DC["date"]))[0]

            # Find the longitude and latitude
            long = list(event.body.objects(tweet_id, GEO["long"]))[0]
            lat = list(event.body.objects(tweet_id, GEO["lat"]))[0]

            # Find the Geonames information associated to the coordinates
            place = geo.findNearbyPlaceName(long, lat)
            geoId = None
            if place != None:                
                (geoId, toponym, country) = place
                event.body.add((tweet_id, GEONAMES["geonameId"], Literal(str(geoId))))
                event.body.add((tweet_id, GEONAMES["toponymName"], Literal(toponym)))
                event.body.add((tweet_id, GEONAMES["countryCode"], Literal(str(country))))                        

            # Forward the modified event
            if onlyWatched:
                if (geoId in watchedIds) or (country in watchedCountry):
                    publisher.publish(event)
                    print event
            else:
                publisher.publish(event)
                print event


if __name__ == "__main__":
    main()
