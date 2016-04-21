# -*- coding: utf-8 -*-
# ztreamy: a framework for publishing semantic events on the Web
# Copyright (C) 2015 Jesus Arias Fisteus
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

import re

_content_type_re = re.compile('{0}/{0}$'.format(r'[^\x01-\x20\x7f'
                                                r'\(\)<>@,;:\\/\[\]\?\{\}]*'))

def get_content_type(value):
    if value:
        parts = value.split(';')
        content_type = parts[0].strip()
        if not _content_type_re.match(content_type):
            raise ValueError('Bad content type')
    else:
        content_type = None
    return content_type
