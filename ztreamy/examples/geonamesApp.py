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

""" This application listens to a stream of geolocated tweets (produced for instance 
    with twitterSensor.py) searches for the geonamesId associated to the tweet 
    location coordinates and in case the geonamesId is included in a list
    of identifiers to be watched, publishes the tweet (enrichted with GeoNames metadata) 
    in another event stream.

    Alternatively, instead of providing a list of identifiers to be watched,
    a country code can be used to define the geographic area of interest.
"""

from __future__ import print_function

import sys
import argparse
import re
import string

from ztreamy.examples.utils.geonamesClient import GeonamesClient
from ztreamy.client import SynchronousClient

from ztreamy import rdfevents
from ztreamy import client

from rdflib import Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef


def queryParser(value):    
    return string.translate(value, None, "{}").split(",")


def main():
    
    # Some useful namespace declarations
    GEONAMES = Namespace("http://www.geonames.org/ontology#") 
    GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
    DC = Namespace("http://purl.org/dc/elements/1.1/")

    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user", dest="user", required=True, 
                  help="the identifier of a registered GeoNames user (e.g. demo)")
    parser.add_argument("-w", "--watched", dest="watched", action='store_const', const=True, default=False,
                  help="a boolean flag indicating whether all tweets, or only those whose location is in the watched list should be published")
    parser.add_argument("-t", "--type", dest="type", required=True, choices=["id","country","position"],
                  help="query type to be used to select the relevant tweets (country|id|position)")
    parser.add_argument("-q", "--query", dest="query", required=True, type=queryParser,
                  help="query to be used to select the relevant tweets (e.g. US, 6545086, '{-3.764647,40.332020,10}')")
    parser.add_argument("-i", "--input", dest="input", required=True,
                  help="URL for input stream where events are read (e.g. http://localhost:9001/events/long-polling)")
    parser.add_argument("-o", "--output", dest="output", required=True,
                  help="URL for output stream where events are published (e.g. http://localhost:9002/events/publish)")
    options = parser.parse_args()

    user = options.user
    inputUrl = options.input
    outputUrl = options.output
    queryType = options.type
    queryValue = options.query
    onlyWatched = options.watched

    # Check query format
    if queryType == "id":
        try:
            if len(queryValue) != 1: 
                raise Exception("Invalid Id")
            int(queryValue[0])
        except:
            print('The GeoNames identifier in id queries should be an integer number, e.g. 6544487, change -q argument')
            return

    if queryType == "country":
        try:
            if (len(queryValue) != 1) or not re.match("^[A-Z]{2}$", queryValue[0]): 
                raise Exception("Invalid Coutry Code")
        except:
            print('Use ISO-3166 2 digit country codes in country queries, e.g. US, change -q argument')
            return

    if queryType == "position":
        try:
            if len(queryValue != 3): 
                raise Exception("Invalid Area")
            (long,lat,radius) = tuple(queryValue)
            float(long)
            float(lat)
            float(radius)
        except:
            print('The position query should provide three arguments, longitude, latitude and radius, e.g. "{-3.764647,40.332020,10}", change the -q argument')

    # Client to listen to geolocated tweet stream
    clnt = SynchronousClient(inputUrl)

    # Access the geonames API
    geo = GeonamesClient(username = user)

    # Case 1: query by list of geonamesIds
    watchedIds = []
    if queryType == "id":
        # Expand the input geonamesId to a list of relevant ids to be watched
        # watchedIds = geo.children(int(queryValue), godown=1)
        #
        # Mockup for Madrid area, previously obtained by calling: children(3117735, godown=1)
        watchedIds = [6545086, 6544487, 6545080, 6947399, 6545095, 6544493, 3125239, 6544492, 6545081, 6544494, \
                           124964, 6545089, 6545079, 6545097, 3123115, 3120635, 3119589, 6545077, 6545082, 3118903, \
                           6545078, 6545084, 6544099, 6545085, 3116156, 6545090, 6545088, 3113943, 6324376, 3119198, \
                           3112772, 3112737, 6544495, 6544491, 6545087, 6545083, 3108118, 6544490, 3106970]
        
        print("List of geonamesIds to be watched: {}".format(watchedIds))

    # Case 2: query by position and radius
    if queryType == "position":
        # Find geonamesId near the position
        (long,lat,radius) = tuple(queryValue)
        watchedIds = geo.findNearbyPlaceNames(long, lat, radius, maxResults = 30)
        # Mockup for Madrid city center obtained by calling findNearbyPlaceNames("-3.704211", "40.416992", radius=1, maxResults=10)
        # watchedIds = [6545083, 3117735, 6544494, 6545088, 6545077, 6545082, 6545081, 6545084]
        print("List of geonamesIds to be watched: {}".format(watchedIds))
    
    # Case 3: query by country code
    watchedCountry = []
    if queryType == "country":
        watchedCountry.append(queryValue[0])
        print("Country code to be watched {}".format(watchedCountry))

    # Publisher to push the generated events 
    publisher = client.SynchronousEventPublisher(outputUrl)

    counter = 0
    
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
                    print(event)
                    counter += 1
                    print("*** {} events published\n".format(counter))
            else:
                publisher.publish(event)
                print(event)
                counter += 1
                print("*** {} events published\n".format(counter))


if __name__ == "__main__":
    main()
