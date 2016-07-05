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

import dateutil

import ztreamy


class TestDates(unittest.TestCase):

    def test_date_parsing(self):
        date = '1970-01-01T10:45:02Z'
        timestamp = ztreamy.parse_timestamp(date)
        self.assertEqual(timestamp, 38702.0)

        date = '1970-01-01T12:45:02+02:00'
        timestamp = ztreamy.parse_timestamp(date)
        self.assertEqual(timestamp, 38702.0)

        date = '1971-01-01T00:00:00+00:00'
        timestamp = ztreamy.parse_timestamp(date)
        self.assertEqual(timestamp, 31536000.0)

        date = '2016-04-06T11:15:02Z'
        timestamp = ztreamy.parse_timestamp(date)
        self.assertEqual(timestamp, 1459941302.0)

    def test_date_parsing_no_tz(self):
        date = '2016-04-06T11:15:02'
        with self.assertRaises(ztreamy.ZtreamyException):
            ztreamy.parse_timestamp(date)

        default_tz = dateutil.tz.tz.tzutc()
        timestamp = ztreamy.parse_timestamp(date, default_tz=default_tz)
        self.assertEqual(timestamp, 1459941302.0)

        default_tz = dateutil.tz.tz.tzoffset(None, -3600)
        timestamp = ztreamy.parse_timestamp(date, default_tz=default_tz)
        self.assertEqual(timestamp, 1459941302.0 + 3600)
