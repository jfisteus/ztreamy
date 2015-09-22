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

""" Contains utility classes to implement a basic client to GeoNames API offering access
    to some methods used within other example applications
"""

from __future__ import print_function

import pycurl
import cjson as json
import cStringIO    
import datetime
import argparse
import time

#
# TODO: This should be a singleton
#
class GateKeeper():
    """ Control access to the geonames API by limiting the maximum number of credits
        expended on a timeWindow defined in seconds
    """
    def __init__(self, credits, timeWindow):
        
        self.credits = credits
        self.toExpend = credits
        self.timeWindow = timeWindow
        self.lastSync = datetime.datetime.now()

    def canIaccess(self, cost):
			
        now = datetime.datetime.now()
        if (now - self.lastSync).seconds >= self.timeWindow:
            self.toExpend = self.credits
            self.lastSync = now
		
        if (self.toExpend - cost) >= 0:	
            self.toExpend -= cost		
            return True
        else:
            return False

    def savings(self):
        return self.toExpend

    def expend(self, cost):
        self.toExpend -= cost


class GeonamesClient():
    """ A utility class that implements a basic client to GeoNames API offering access
        to some methods used within other example applications
    """
    def __init__(self, username = "demo"):

        self.HOURLIMITERR = 19
        self.DAYLIMITERR = 18
        self.BASEURL = "http://api.geonames.org/"		

        self.username = username
        # Control limits of the free API access, see: http://www.geonames.org/export/#ws
        self.hourlyGateKeeper = GateKeeper(1800, 3600) # Theoretically 2000 per hour
        self.dailyGateKeeper = GateKeeper(28000, 3600*24) # Theoretically 30k per day
        # TODO: Add cache		
        # self.cache = {}


    # Cost = two credits per call see: http://www.geonames.org/export/credits.html
    def findNearbyWikipedia(self, longitude, latitude, credits = 2):		

        # Default URL to answer: Google Maps one
        result = "http://maps.google.es/maps?f=q&hl=es&geocode=&q=" + longitude + "," + latitude + "&ie=UTF8"
        
        # Call the service
        serviceUrl = self.BASEURL + "findNearbyWikipediaJSON"
        params = "?lat=" + str(latitude) + "&lng=" + str(longitude) + "&maxRows=1&username=" + self.username
        url = serviceUrl + params
        
        # TODO: Add cache
        # if url in self.cache:
        #	return self.cache[url]
        
        content = ""
        if self.hourlyGateKeeper.canIaccess(credits) and self.dailyGateKeeper.canIaccess(credits):
            storage = cStringIO.StringIO()
            self.callService(url, storage)
            content = storage.getvalue()
            storage.close()

        # Read the answer
        if content != "":
            geo_dict = json.decode(content)
            # Check for geonames errors: http://www.geonames.org/export/webservice-exception.html
            if not self.checkAPIerror(geo_dict):
                if "geonames" in geo_dict:
                    if len(geo_dict["geonames"]) > 0:
                        entry = geo_dict["geonames"][0]
                        result = "http://" + entry["wikipediaUrl"]
                        # TODO: Add cache
                        # self.cache[url] = result
			
        return result


    # Cost = three credits per call see: http://www.geonames.org/export/credits.html
    def findNearbyPlaceName(self, longitude, latitude, credits = 3):		

        # Default URL to answer: Google Maps one
        result = None

        # Call the service
        serviceUrl = self.BASEURL + "findNearbyPlaceNameJSON"
        params = "?lat=" + str(latitude) + "&lng=" + str(longitude) + "&maxRows=1&username=" + self.username
        url = serviceUrl + params

        # TODO: Add cache
        # if url in self.cache:
        #	return self.cache[url]

        content = ""
        if self.hourlyGateKeeper.canIaccess(credits) and self.dailyGateKeeper.canIaccess(credits):
            storage = cStringIO.StringIO()
            self.callService(url, storage)
            content = storage.getvalue()
            storage.close()

        # Read the answer
        if content != "":
            geo_dict = json.decode(content)
            # Check for geonames errors: http://www.geonames.org/export/webservice-exception.html
            if not self.checkAPIerror(geo_dict):
                if "geonames" in geo_dict:
                    if len(geo_dict["geonames"]) > 0:
                        entry = geo_dict["geonames"][0]
                        geoId = entry["geonameId"]					
                        toponym = entry["toponymName"]					
                        country = entry["countryCode"]
                        result = (geoId, toponym, country)					
                        # TODO: Add cache
                        # self.cache[url] = result
			
        return result


    # Cost = three credits per call see: http://www.geonames.org/export/credits.html
    def findNearbyPlaceNames(self, longitude, latitude, radius = 10, maxResults = 10, credits = 3):		

        # Default URL to answer: Google Maps one
        result = []
        
        # Call the service
        serviceUrl = self.BASEURL + "findNearbyPlaceNameJSON"
        params = "?lat=" + str(latitude) + "&lng=" + str(longitude) + "&radius=" + str(radius) + "&maxRows=" + str(maxResults) + "&username=" + self.username
        url = serviceUrl + params
        
        # TODO: Add cache
        # if url in self.cache:
        #	return self.cache[url]
        
        content = ""
        if self.hourlyGateKeeper.canIaccess(credits) and self.dailyGateKeeper.canIaccess(credits):
            storage = cStringIO.StringIO()
            self.callService(url, storage)
            content = storage.getvalue()
            storage.close()

        # Read the answer
        if content != "":
            geo_dict = json.decode(content)
            # Check for geonames errors: http://www.geonames.org/export/webservice-exception.html
            if not self.checkAPIerror(geo_dict):
                if "geonames" in geo_dict:
                    entries = len(geo_dict["geonames"])
                    for idx in range(0, entries):
                        entry = geo_dict["geonames"][idx]
                        geoId = entry["geonameId"]					
                        result.append(geoId)					
                        # TODO: Add cache
                        # self.cache[url] = result
			
        return result

    # Cost = one credit per call see: http://www.geonames.org/export/credits.html
    def children(self, id, godown = 1, maxResults = 200, credits = 1):		

        # Default URL to answer: Google Maps one
        result = []
        
        # Call the service
        serviceUrl = self.BASEURL + "childrenJSON"
        params = "?geonameId=" + str(id) + "&maxRows=" + str(maxResults) + "&username=" + self.username
        url = serviceUrl + params
        
        # TODO: Add cache
        # if url in self.cache:
        #	return self.cache[url]
        
        content = ""
        if self.hourlyGateKeeper.canIaccess(credits) and self.dailyGateKeeper.canIaccess(credits):
            storage = cStringIO.StringIO()
            self.callService(url, storage)
            content = storage.getvalue()
            storage.close()
            
        # Read the answer
        if content != "":
            geo_dict = json.decode(content)
            # Check for geonames errors: http://www.geonames.org/export/webservice-exception.html
            if not self.checkAPIerror(geo_dict):
                if "geonames" in geo_dict:
                    entries = len(geo_dict["geonames"])
                    for idx in range(0, entries):
                        entry = geo_dict["geonames"][idx]
                        geoId = entry["geonameId"]					
                        result.append(geoId)					
                        if (godown > 0) and "numberOfChildren" in entry:
                            hasChildren = int(entry["numberOfChildren"])
                            if hasChildren > 0:
                                result.extend(self.children(geoId,godown-1))
                                # TODO: Add cache
                                # self.cache[url] = result
			
        return result


    def checkAPIerror(self, data):

        if "status" in data:
            entry = data["status"]		
            errorCode = entry["value"]

            # Sync the GateKeeper by expending all savings (thus disallowing future calls for a while)
            if errorCode == self.HOURLIMITERR:		
                self.hourlyGateKeeper.expend(self.hourlyGateKeeper.savings())
                print("Hour limit error")

            if errorCode == self.DAYLIMITERR:
                self.dailyGateKeeper.expend(self.dailyGateKeeper.savings())
                print("Day limit error")
                
            return True
        
        else:
            return False		


    def callService(self, url, storage):

        conn = pycurl.Curl()
        conn.setopt(pycurl.CONNECTTIMEOUT, 30)
        conn.setopt(pycurl.TIMEOUT, 300)
        conn.setopt(pycurl.URL, url)
        conn.setopt(pycurl.WRITEFUNCTION, storage.write)

        try:
            conn.perform()
            conn.close()
        except pycurl.error, error:
            print("Error while accessing GeoNames")
            
        return storage


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user", dest="user", required=True, 
                        help="a GeoNames account login (e.g. demo)")    
    options = parser.parse_args()

    client = GeonamesClient(username = options.user)
    print(client.findNearbyWikipedia("-3.764647", "40.332020"))
    print(client.findNearbyPlaceName("-3.764647", "40.332020"))
    print(client.findNearbyPlaceNames("-3.704211", "40.416992", radius=1))
    print(client.children(3117735, godown=1))


if __name__ == "__main__":
    main()
