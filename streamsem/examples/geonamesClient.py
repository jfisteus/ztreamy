import pycurl
import cjson as json
import cStringIO    
import datetime
import time

#
# Control access to the geonames API by limiting the maximum number of credits
# expended on a timeWindow defined in seconds
#
class GateKeeper():

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


	def __init__(self, username = "demo"):

		self.HOURLIMITERR = 19
		self.DAYLIMITERR = 18
		self.BASEURL = "http://api.geonames.org/"		

		self.username = username
		# Control limits of the free API access, see: http://www.geonames.org/export/#ws
		self.hourlyGateKeeper = GateKeeper(1500, 3600) # Theoretically 2000 per hour
		self.dailyGateKeeper = GateKeeper(25000, 3600*24) # Theoretically 30k per day
		# TODO: Add cache		
		# self.cache = {}

	# Cost = two credits per call see: http://www.geonames.org/export/credits.html
	def findNearbyWikipedia(self, longitude, latitude, credits = 2):

		# Default URL to answer: Google Maps one
		result = "http://maps.google.es/maps?f=q&hl=es&geocode=&q=" + longitude + "," + latitude + "&ie=UTF8"

		# Call the service
		serviceUrl = self.BASEURL + "findNearbyWikipediaJSON"
		params = "?lat=" + latitude + "&lng=" + longitude + "&maxRows=1&username=" + self.username
		url = serviceUrl + params

		# TODO: Add cache
		# if url in self.cache:
		#	return self.cache[url]

		content = ""
		if self.hourlyGateKeeper.canIaccess(credits) and self.dailyGateKeeper.canIaccess(credits):
			storage = cStringIO.StringIO()
			self.callService(url, storage)
			content = storage.getvalue()

		# Read the answer
		if content != "":
			geo_dict = json.decode(content)
			# Check for geonames errors: http://www.geonames.org/export/webservice-exception.html
			if "status" in geo_dict:
				entry = geo_dict["status"][0]		
				errorCode = entry["value"]

				if errorCode == HOURLIMITERR:		
					self.hourlyGateKeeper.expend(self.hourlyGateKeeper.savings())
				if errorCode == DAYLIMITERR:
					self.dailyGateKeeper.expend(self.dailyGateKeeper.savings())
		   	else: 
				if "geonames" in geo_dict:
					entry = geo_dict["geonames"][0]
					result = "http://" + entry["wikipediaUrl"]
					# TODO: Add cache
					# self.cache[url] = result
			storage.close()

		return result

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
		    print "Error while accessing GeoNames"

		return storage

def main():
	client = GeonamesClient(username = "dummy")
	print client.findNearbyWikipedia("-3.764647", "40.332020")

if __name__ == "__main__":
    main()
