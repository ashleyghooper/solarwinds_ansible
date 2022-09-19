# -*- coding: utf-8 -*-
#
# Copyright: (c) 2022, Ashley Hooper <ashleyghooper@gmail.com>
#
# GNU General Public License v3.0+
# (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

from datetime import timedelta, tzinfo

ZERO = timedelta(0)
HOUR = timedelta(hours=1)


class UTC(tzinfo):
    """UTC timezone implementation for Python < 3.2"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO
