# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2011-2015 Jesus Arias Fisteus
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
from __future__ import print_function

import os
import sys
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

if sys.version_info[0] != 2 or sys.version_info[1] not in [7]:
    print('ztreamy needs Python 2.7')
    sys.exit(1)

# Dependencies
requirements = ['setuptools',
                'tornado',
                'rdflib>=4.0',
                'rdflib-jsonld',
                'pycurl',
                'pyparsing',
                'simplejson',
                'futures',
                'python-dateutil',
                ]

tests_require = []

setup(
    name = "ztreamy",
    version = "0.5",
    author = "Jesus Arias Fisteus",
    author_email = "jfisteus@gmail.com",
    description = ("A framework for publishing semantic events on the Web"),
    license = 'GPL3',
    keywords = "rdf sensors web semantic-sensor-web",
    url = "http://www.ztreamy.org/",
    packages=['ztreamy', 'ztreamy.utils', 'ztreamy.tools',
              'ztreamy.examples', 'ztreamy.experiments',
              'ztreamy.casestudy'],
    package_data={'ztreamy': ['data/static/*']},
    long_description=read('README'),
    install_requires = requirements,
    tests_require = tests_require,
    test_suite = "tests.get_tests",
)
