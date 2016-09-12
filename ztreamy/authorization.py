# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2016 Pablo Crespo Bellido
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
""" Implementation of authorization based on IP whitelisting.

The IPy module is used to manage IP addresses. 

The methods implemented in this module are  used in the 
module server.py to allow a client publish and/or subscribe 
streams.

"""

from IPy import IP, IPSet

class IPAuthorizationManager(object):
    def __init__(self, whitelist=None):
        self.whitelist = IPSet()
        if whitelist is not None:
            self.load_from_list(whitelist)

    def load_from_list(self, whitelist):
        for ip_exp in whitelist:
            self.whitelist.add(IP(ip_exp))

    def load_from_file(self, filename):
        with open(filename) as f:
            for line in f:
                self.whitelist.add(IP(line.strip()))

    def authorize_ip(self, ip):
        ip_aux = IPSet([IP(ip)])
        if ip_aux.isdisjoint(self.whitelist)==True:
            return False
        return True

    def authorize(self, request):
        return self.authorize_ip(request.remote_ip)
