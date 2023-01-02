#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2022, Ashley Hooper <ashleyghooper@gmail.com>
# Copyright: (c) 2019, Jarett D. Chaiken <jdc@salientcg.com>
# GNU General Public License v3.0+
# (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type


DOCUMENTATION = r"""
---
module: solarwinds_info

short_description: Retrieve information from SolarWinds

description:
  - Retrieve information from SolarWinds using the
    L(SolarWinds Information Service,
    https://solarwinds.github.io/OrionSDK/2020.2/schema/Orion.Nodes.html)\
    (SWIS).
  - All options that use the 'str' data type use the SQL 'LIKE' operator.
    This means they will accept the standard SQL wildcard '%' for partial
    matching, but if the string does not contain the '%' wildcard, only
    exact matching will be performed. WARNING! Be very careful using
    wildcards, since overuse of wildcards may affect performance of the
    SolarWinds system/SQL server and may take a very long time to run!
    Be attentive to any queries that take more than a few seconds to run.
  - If running against an Ansible inventory rather than localhost, consider
    using the 'throttle' option on the task to avoid overloading the SWIS SQL
    server.
  - When multiple options are provided, the intersection - in other words,
    the nodes that match all of the options - is returned.
  - When multiple values are provided for a single option, matching is against
    any one of these values.
  - SWIS table and column (property) names match the SWIS schema (see above
    link), although it should not be necessary for case to match.
  - Unfortunately, it's not a straightforward process to reliably determine the
    Operating System of a node. Some SWIS columns that may be of interest are
    Vendor, MachineType, NodeDescription. For nodes using SolarWinds agents,
    there are also OSDistro, RuntimeOSDistro, RuntimeOSLabel, and OSLabel in
    the Orion.AgentManagement.Agent table.

extends_documentation_fragment:
  - anophelesgreyhoe.solarwinds.solarwinds_client

version_added: "2.0.0"

author:
  - "Ashley Hooper (@ashleyghooper)"

options:
  base_table:
    description:
      - The table to serve as the primary information source for the query.
  columns:
    description:
      - Specification of which columns to include in output, in the form of a
        dict of table names, with each element either being empty (in
        which case all columns will be returned), or a list of columns.
    type: dict
  include:
    description:
      - Specification of filters for inclusion of data, in the form of a dict
        of table names, with each element consisting of a dict of column names,
        each containing either a single value or a list of values.
        Additionally, there are two special subelements, 'min' and 'max'
        which enable specifying ranges of values.
        columns.
    type: dict
  exclude:
    description:
      - Specification of filters for exclusion of data, in the form of a dict
        of table names, with each element consisting of a dict of column names,
        each containing either a single value or a list of values.
        Additionally, there are two special subelements, 'min' and 'max'
        which enable specifying ranges of values.
    type: dict

requirements:
  - "python >= 2.6"
  - requests
  - traceback
"""

EXAMPLES = r"""
- name: Find all nodes that are polled using SNMP v1 or v2
  hosts: localhost
  gather_facts: no
  tasks:
    - name:  Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.solarwinds_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        base_table: Orion.Nodes
        columns:
          Orion.Nodes:
        include:
          Orion.Nodes:
            ObjectSubType: SNMP
            SNMPVersion:
              min: 1
              max: 2
      delegate_to: localhost

- name: Find all nodes in Australia with IP addresses starting with '10.100.0.'
  hosts: localhost
  gather_facts: no
  tasks:
    - name:  Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.solarwinds_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        base_table: Orion.Nodes
        columns:
          Orion.Nodes:
          Orion.NodesCustomPropeties:
            - Country
        include:
          Orion.Nodes:
            IPAddress: "10.100.0.%"
          Orion.NodesCustomPropeties:
            Country: Australia
      delegate_to: localhost

- name: Find all nodes currently having severity of 100 or higher
  hosts: localhost
  gather_facts: no
  tasks:
    - name:  Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.solarwinds_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        base_table: Orion.Nodes
        columns:
          Orion.Nodes:
        include:
          Orion.Nodes:
            Severity:
              min: 100
      delegate_to: localhost

- name: Find nodes in Australia with current status of 'Down'
  hosts: localhost
  gather_facts: no
  tasks:
    - name:  Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.solarwinds_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        base_table: Orion.Nodes
        columns:
          Orion.Nodes:
            - NodeID
            - Caption
            - IP
            - StatusDescription
          Orion.NodesCustomProperties:
            - Country
          Orion.StatusInfo:
            - StatusName
        include:
          Orion.StatusInfo:
            StatusName: Down
        exclude:
          Orion.NodesCustomProperties:
            Country: Australia
      delegate_to: localhost
"""

# TODO: Add Ansible module RETURN section

import traceback

from ansible.module_utils.basic import AnsibleModule, missing_required_lib
from ansible.module_utils._text import to_native
from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.solarwinds_client import (
    SolarWindsClient,
    solarwindsclient_argument_spec,
)

from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.solarwinds_query import (
    SolarWindsQuery,
)

REQUESTS_IMPORT_ERROR = None
try:
    import requests

    requests.urllib3.disable_warnings()
except ImportError:
    HAS_REQUESTS = False
    REQUESTS_IMPORT_ERROR = traceback.format_exc()
else:
    HAS_REQUESTS = True


class SolarWindsInfo(object):
    """
    Retrieve information from the SolarWinds Information Service (SWIS).
    """

    def __init__(self, solarwinds):
        self.solarwinds = solarwinds
        self.changed = False

    def info(self, module):
        params = module.params
        solarwinds_query = SolarWindsQuery(module, self.solarwinds.client)
        return solarwinds_query.query(
            params["base_table"],
            params["columns"],
            params["include"],
            module.params["exclude"],
        )
        # TODO: Clean up
        query_res = query.execute()
        return query_res
        query = SolarWindsQuery(module, self.solarwinds.client)
        query.base_table = module.params["base_table"]
        query.input_columns = module.params["columns"]
        query.input_include = module.params["include"]
        query.input_exclude = module.params["exclude"]
        query_res = query.execute()
        return query_res


# ==============================================================
# main


def main():

    argument_spec = dict(
        base_table=dict(
            type="str",
        ),
        columns=dict(
            type="dict",
        ),
        include=dict(
            type="dict",
        ),
        exclude=dict(
            type="dict",
        ),
    )

    argument_spec.update(solarwindsclient_argument_spec())

    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
    )

    if not HAS_REQUESTS:
        module.fail_json(
            msg=missing_required_lib("requests"), exception=REQUESTS_IMPORT_ERROR
        )

    solarwinds = SolarWindsClient(module)
    info = SolarWindsInfo(solarwinds)

    res_args = dict(changed=False, info=info.info(module))

    module.exit_json(**res_args)


if __name__ == "__main__":
    main()
