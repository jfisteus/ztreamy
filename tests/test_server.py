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

from tornado.web import HTTPError

from ztreamy.server import _GenericHandler

class TestServer(unittest.TestCase):

    def test_parse_content_encoding(self):
        value = ['identity']
        self.assertEqual(_GenericHandler._accept_values_internal(value),
                         ['identity'])

        value = ['deflate, identity']
        self.assertEqual(_GenericHandler._accept_values_internal(value),
                         ['deflate', 'identity'])

        value = ['deflate;q=0.5, identity;q=1.0']
        self.assertEqual(_GenericHandler._accept_values_internal(value),
                         ['identity', 'deflate'])

        value = ['identity;q=1.0, deflate;q=0.5']
        self.assertEqual(_GenericHandler._accept_values_internal(value),
                         ['identity', 'deflate'])

        value = ['deflate,identity; q=0.5']
        self.assertEqual(_GenericHandler._accept_values_internal(value),
                         ['deflate', 'identity'])

        value = ['identity;q=1.0000, deflate;q=0.5']
        self.assertRaises(HTTPError,
                          _GenericHandler._accept_values_internal, value)

        value = ['identity;q=1.000, deflate;q=1.0001']
        self.assertRaises(HTTPError,
                          _GenericHandler._accept_values_internal, value)

        value = ['identity;q=1.000, deflate;q=']
        self.assertRaises(HTTPError,
                          _GenericHandler._accept_values_internal, value)
