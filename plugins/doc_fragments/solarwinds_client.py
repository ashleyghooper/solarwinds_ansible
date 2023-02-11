# -*- coding: utf-8 -*-

# Copyright (c) 2017, Daniel Korn <korndaniel1@gmail.com>
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import (absolute_import, division, print_function)

__metaclass__ = type


class ModuleDocFragment(object):

    # Standard SolarWinds documentation fragment
    DOCUMENTATION = r'''
options:
  solarwinds_connection:
    description:
      - SolarWinds connection configuration information.
    required: false
    type: dict
    suboptions:
      hostname:
        description:
          - Name of Orion host running SWIS service.
        type: str
        required: true

      username:
        description:
          - Orion Username.
          - Active Directory users may use C(DOMAIN\\username) or C(username@DOMAIN) format.
        type: str
        required: true

      password:
        description:
          - Password for Orion user.
        type: str
        required: true

requirements:
  - 'orionsdk U(https://github.com/solarwinds/OrionSDK)'
'''
