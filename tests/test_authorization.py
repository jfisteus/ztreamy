# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2014-2015 Jesus Arias Fisteus
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

import unittest

import ztreamy.authorization as authorization

from IPy import IP


class TestAuthorization(unittest.TestCase):

    def test_authorize_ipv4(self):
        w = [IP('165.2.3.0'), IP('10.128.1.253'), IP('175.0.2.0'),\
            IP('197.0.0.1'), IP('100.2.3.0'), IP('83.128.0.0')]
        whitelist = authorization.IPAuthorizationManager(w)
        self.assertEqual(whitelist.authorize_ip('165.2.3.0'), True)
        self.assertEqual(whitelist.authorize_ip('10.128.1.253'), True)
        self.assertEqual(whitelist.authorize_ip('175.0.2.0'), True)
        self.assertEqual(whitelist.authorize_ip('197.0.0.1'), True)
        self.assertEqual(whitelist.authorize_ip('100.2.3.0'), True)
        self.assertEqual(whitelist.authorize_ip('83.128.0.0'), True)
        self.assertEqual(whitelist.authorize_ip('50.15.0.0'), False)
        self.assertEqual(whitelist.authorize_ip('255.128.0.253'), False)
        self.assertEqual(whitelist.authorize_ip( \
                      '1993:0db8:85a3::1319:8a2e:0370:7344'), False)
        self.assertEqual(whitelist.authorize_ip( \
                              '756:0DBB::15:0000:1900:cdab'), False)

    def test_authorize_ipv4_netmask(self):
        w = [IP('165.2.3.0/30'), IP('10.128.0.0/28'), IP('175.0.0.0/22')]
        whitelist = authorization.IPAuthorizationManager(w)
        self.assertEqual(whitelist.authorize_ip('165.2.3.0'), True)
        self.assertEqual(whitelist.authorize_ip('165.2.3.1'), True)
        self.assertEqual(whitelist.authorize_ip('165.2.3.2'), True)
        self.assertEqual(whitelist.authorize_ip('165.2.3.3'), True)
        self.assertEqual(whitelist.authorize_ip('165.2.3.4'), False)
        self.assertEqual(whitelist.authorize_ip('165.2.4.4'), False)
        self.assertEqual(whitelist.authorize_ip('10.128.0.0'), True)
        self.assertEqual(whitelist.authorize_ip('10.128.0.1'), True)
        self.assertEqual(whitelist.authorize_ip('10.128.0.15'), True)
        self.assertEqual(whitelist.authorize_ip('10.128.0.16'), False)
        self.assertEqual(whitelist.authorize_ip('10.128.1.16'), False)
        self.assertEqual(whitelist.authorize_ip('175.0.0.0'), True)
        self.assertEqual(whitelist.authorize_ip('175.0.1.155'), True)
        self.assertEqual(whitelist.authorize_ip('175.0.2.10'), True)
        self.assertEqual(whitelist.authorize_ip('175.0.3.90'), True)
        self.assertEqual(whitelist.authorize_ip('175.0.3.255'), True)
        self.assertEqual(whitelist.authorize_ip('175.0.4.0'), False)
        self.assertEqual(whitelist.authorize_ip( \
                      '2001:0db8:85a3::1319:8a2e:0370:7344'), False)
        self.assertEqual(whitelist.authorize_ip( \
                          '2000:0DB8:0000:1408::1428:57a5'), False)

    def test_authorize_ipv6(self):
        w = [IP('2001:0123:0004:00ab:0cde:3403:0001:0063'), \
             IP('2001:0:0:0:0:0:0:4'), \
             IP('2001:0db8:85a3:0000:1319:8a2e:0370:7344'), \
             IP('2016:0DB8:0000:0000:0000:0000:1428:57ab'), \
             IP('2001:DB8:02de::0e13'), \
             IP('2001:db8:3c4d:15:0:d234:3eee::')]
        whitelist = authorization.IPAuthorizationManager(w)
        self.assertEqual(whitelist.authorize_ip( \
                            '2001:123:4:ab:cde:3403:1:63'), True)
        self.assertEqual(whitelist.authorize_ip('2001::4'), True)
        self.assertEqual(whitelist.authorize_ip( \
                    '2001:0db8:85a3::1319:8a2e:0370:7344'), True)
        self.assertEqual(whitelist.authorize_ip( \
                '2016:0DB8:0000:0000:0000:0000:1428:57ab'), True)
        self.assertEqual(whitelist.authorize_ip( \
                    '2016:0DB8:0000:0000:0000::1428:57ab'), True)
        self.assertEqual(whitelist.authorize_ip( \
                            '2016:0DB8:0:0:0:0:1428:57ab'), True)
        self.assertEqual(whitelist.authorize_ip( \
                               '2016:0DB8:0::0:1428:57ab'), True)
        self.assertEqual(whitelist.authorize_ip( \
                                   '2016:0DB8::1428:57ab'), True)
        self.assertEqual(whitelist.authorize_ip('2001:0DB8:2de::e13'), True)
        self.assertEqual(whitelist.authorize_ip('2001::454'), False)
        self.assertEqual(whitelist.authorize_ip( \
                  '2001:0db8:3c4d:0015:0000:0000:1a2f:1a2b'), False)
        self.assertEqual(whitelist.authorize_ip( \
                            '2011:db8:3c4:18:0:d234:3eee::'), False)
        self.assertEqual(whitelist.authorize_ip( \
               '2001:0db8:3c4d:0015:0000:d234:3eee:0000'), True)
        self.assertEqual(whitelist.authorize_ip( \
                                        '11:db8:3c4:3eee::'), False)
        self.assertEqual(whitelist.authorize_ip('250.15.0.10'), False)
        self.assertEqual(whitelist.authorize_ip('197.0.0.15'), False)

    def test_authorize_ipv6_netmask(self):
        w = [IP('2652:1500:0000:0000:5000:0000:0000:0000/120'),\
             IP('175.0.2.0'), IP('197.0.0.1'), IP('4000::/116'),
             IP('100.2.3.0'), IP('83.128.0.0')]
        whitelist = authorization.IPAuthorizationManager(w)
        self.assertEqual(whitelist.authorize_ip('4000::1'), True)
        self.assertEqual(whitelist.authorize_ip('4000::ff'), True)
        self.assertEqual(whitelist.authorize_ip('4000::500'), True)
        self.assertEqual(whitelist.authorize_ip('4000::ffff:ff0a'), False)
        self.assertEqual(whitelist.authorize_ip('4000::f'), True)
        self.assertEqual(whitelist.authorize_ip('4000::fff'), True)
        self.assertEqual(whitelist.authorize_ip('4000::ffff'), False)
        self.assertEqual(whitelist.authorize_ip('2652:1500::5000:0:0:1'), \
                                                              True)
        self.assertEqual(whitelist.authorize_ip('2652:1500::5000:0:0:ff'), \
                                                              True)
        self.assertEqual(whitelist.authorize_ip('2653:1500::8000:0:0:ff'), \
                                                              False)
        self.assertEqual(whitelist.authorize_ip('2652:1500::5000:0:0:1ff'), \
                                                              False)
        self.assertEqual(whitelist.authorize_ip('50.15.0.0'), False)
        self.assertEqual(whitelist.authorize_ip('255.128.0.253'), False)
