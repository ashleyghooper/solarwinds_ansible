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
  - All options that use the C(str) data type use the SQL C(LIKE) operator.
    This means they will accept the standard SQL wildcard C(%) for partial
    matching, but if the string does not contain the C(%) wildcard, only
    exact matching will be performed. WARNING! Be careful using wildcards,
    since poorly specified queries may take a long time to run and their could
    be some performance impact on the SolarWinds system/SQL server. Be
    attentive to any queries that take more than a few seconds to run.
  - Conversely, in many cases, the number of included columns and the sizes
    of values they contain has a greater impact on query run time than the
    filtering/number of rows returned; specifying only the needed columns
    can greatly improve query performance.
  - If running against an Ansible inventory rather than localhost, consider
    using the C(throttle) option on the Ansible task to avoid overloading
    the SWIS SQL server.
  - For each element of the filters argument, the intersection - in other words,
    the set of rows that match all of the options (SQL C(AND)) - is returned.
    For example, if a single filter specifies a partial IP address and custom
    property, all rows which match both conditions are returned. Use multiple
    filter arguments for more complex queries.
  - When multiple values are provided for a single option, matching is against
    any one of these values. For example, if the value for the C(IPAddress)
    filter is a list, then rows matching any of the items in the list are
    returned.
  - Note that if a query explicitly specifies for no columns from the base
    table to be returned (C(columns) is provided but is empty, or contains an
    empty list C([])), but the query does include columns for nested entities,
    all nested entities will be nested on to a single topXlevel object. This
    could be fixed quite trivially if C(SWQL) supported a SQL C(ROWID) or
    similar, but it does not. In the meantime, to avoid this, always specify at
    least a single column from the base table to avoid this behaviour.
  - SWIS table and column (property) names match the SWIS schema (see above
    link), although it should not be necessary for case to match.
  - Unfortunately, it's not a straightforward process to reliably determine the
    Operating System of a node. Some SWIS columns that may be of interest are
    Vendor, MachineType, NodeDescription. For nodes using SolarWinds agents,
    there are also C(OSDistro), C(RuntimeOSDistro), C(RuntimeOSLabel), and
    C(OSLabel) in the C(Orion.AgentManagement.Agent) table.

extends_documentation_fragment:
  - anophelesgreyhoe.solarwinds.solarwinds_client

version_added: "2.0.0"

author:
  - "Ashley Hooper (@ashleyghooper)"

options:
  base_table:
    description:
      - Specification of the SWIS table to base the query upon and the columns
        to be included.
    required: true
    type: dict
    suboptions:
      name:
        description:
          - Name of the SWIS table the query should be based upon.
        type: str
        required: true

      columns:
        description:
          - The list of columns to include in the query.
          - All columns for the base table will be looked up via the metadata
            and included if not provided.
        type: list
        elements: str
        required: false

  nested_entities:
    description:
      - Specification of entities that are accessible from the base table,
        for example C(Agent), C(Interfaces), C(Volumes), etc.
      - Each key should be the relative name of the nested entity, so for
        example, for the base table C(Orion.Nodes), the nested entity
        C(Orion.Nodes.Interfaces) is accessible via C(Interfaces).
      - For each nested entity, individual columns can be specified below
        via the I(columns) option, otherwise all columns will be returned.
    type: dict
    default: {}

  filters:
    description:
      - Filters for inclusion and/or exclusion of records based on values.
      - If multiple filter elements are provided, they function as
        alternatives (that is to say, they are combined using SQL C(OR)).
    type: list
    elements: dict
    suboptions:
      include:
        description:
          - Specification of filters for inclusion of data, in the form of a
            C(dict) containing qualified column names within the base table (for
            example, C(CustomProperties.Country)), each of which specifying a
            single value, a list of values, or a range (see below).
          - Ranges may be specified via two special subelements for any column,
            I(min) and I(max) which enable specifying ranges of values.
        type: dict

      exclude:
        description:
          - Specification of filters for exclusion of data, in the form of a
            C(dict) containing qualified column names within the base table (for
            example, C(CustomProperties.Country)), each of which specifying a
            single value, a list of values, or a range (see below).
          - Ranges may be specified via two special subelements for any column,
            I(min) and I(max) which enable specifying ranges of values.
        type: dict

requirements:
  - "python >= 2.6"
  - requests
  - traceback
"""

EXAMPLES = r"""
- name: Get details of all SolarWinds polling engines
  hosts: localhost
  gather_facts: false
  tasks:
    - name: Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.solarwinds_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        base_table:
          name: Orion.Engines
      delegate_to: localhost

- name: Find all nodes that are polled using SNMP v1 or v2
  hosts: localhost
  gather_facts: false
  tasks:
    - name:  Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.solarwinds_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        base_table:
          name: Orion.Nodes
        filters:
          - include:
              ObjectSubType: SNMP
              SNMPVersion:
                min: 1
                max: 2
      delegate_to: localhost

- name: Find all nodes in Australia with IP addresses starting with '10.100.0.'
  hosts: localhost
  gather_facts: false
  tasks:
    - name:  Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.solarwinds_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        base_table:
          name: Orion.Nodes
        nested_entities:
          Orion.NodesCustomProperties:
            columns:
              - Country
        filters:
          - include:
              IPAddress: "10.100.0.%"
              CustomProperties.Country: Australia
      delegate_to: localhost

- name: Find all nodes currently having severity of 100 or higher
  hosts: localhost
  gather_facts: false
  tasks:
    - name:  Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.solarwinds_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        base_table:
          name: Orion.Nodes
        filters:
          - include:
              Severity:
                min: 100
      delegate_to: localhost

- name: Nodes with status of 'Down' excluding any with custom property Country = 'Australia'
  hosts: localhost
  gather_facts: false
  tasks:
    - name:  Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.solarwinds_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        base_table:
          name: Orion.Nodes
          columns:
            - NodeID
            - Caption
            - IP
            - StatusDescription
        nested_entities:
          CustomProperties:
            columns:
              - Country
        filters:
          - include:
              Status: 2
            exclude:
              CustomProperties.Country: Australia
      delegate_to: localhost
"""

# TODO: Add Ansible module RETURN section

import traceback

from ansible.module_utils.basic import AnsibleModule, missing_required_lib
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
            params["nested_entities"],
            params["filters"],
        )


# ==============================================================
# main


def main():
    argument_spec = dict(
        base_table=dict(
            type="dict",
            options=dict(
                name=dict(type="str", required=True),
                columns=dict(type="list", elements="str", required=False),
            ),
            required=True,
        ),
        nested_entities=dict(
            type="dict",
            default={},
        ),
        filters=dict(
            type="list",
            elements="dict",
            options=dict(
                include=dict(type="dict"),
                exclude=dict(type="dict"),
            ),
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
    solarwinds_info = SolarWindsInfo(solarwinds)
    info = solarwinds_info.info(module)
    res_args = dict(
        changed=False,
        count=len(info["data"]),
        solarwinds_info=info["data"],
    )
    if "queries" in info:
        res_args["queries"] = info["queries"]
    module.exit_json(**res_args)


if __name__ == "__main__":
    main()
