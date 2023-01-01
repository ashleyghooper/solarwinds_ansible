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
module: orion_node_info

short_description: Retrieve information about nodes in Solarwinds Orion NPM

description:
  - Retrieve information about nodes in SolarWinds Orion NPM using the
    L(SolarWinds Information Service,
    https://solarwinds.github.io/OrionSDK/2020.2/schema/Orion.Nodes.html)\
    (SWIS).
  - All options that use the 'str' data type use the SQL 'LIKE' operator.
    This means they will accept the standard SQL wildcard '%' for partial
    matching, but if the string does not contain the '%' wildcard, only
    exact matching will be performed. WARNING! Be very careful using
    wildcards, since overuse of wildcards may place excessive load on the SWIS
    SQL server and could even lead to a SolarWinds outage! Be attentive to any
    queries that take more than a few seconds to run.
  - If running against an Ansible inventory rather than localhost, consider
    using the 'throttle' option on the task to avoid overloading the SWIS SQL
    server.
  - When multiple options are provided, the intersection - in other words,
    the nodes that match all of the options - is returned.
  - When multiple values are provided for a single option, matching is against
    any one of these values.
  - Most options use Ansible standard snake case, but options that directly
    match properties of other entities, such as agents and custom properties,
    use the raw SWIS column names.
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
  filters:
    description:
      - Filters to determine which nodes are included in the query.
    type: dict
    suboptions:
      node_id:
        description:
          - node_id of the node.
        type: list
        elements: int

      dns:
        description:
          - The fully-qualified DNS domain name of the node, or partial DNS name
            if wildcard used.
        type: list
        elements: str

      caption:
        description:
          - The SolarWinds 'caption' for the node.
        type: list
        elements: str

      sys_name:
        description:
          - Local system name as might be returned by the 'hostname' command.
        type: list
        elements: str

      ip_address:
        description:
          - IP address, or partial ip address if wildcard used.
        type: list
        elements: str

      object_sub_type:
        description:
          - The type of node.
        choices: [ "agent", "icmp", "snmp", "wmi" ]
        type: list
        elements: str

      polling_method:
        description:
          - The polling method - alias for object_sub_type.
        choices: [ "agent", "icmp", "snmp", "wmi" ]
        type: list
        elements: str

      polling_engine_id:
        description:
          - Id of the polling engine.
        type: list
        elements: int

      snmp_version:
        description:
          - The version of SNMP, or 0 for non-SNMP.
        choices: [ 0, 1, 2, 3 ]
        type: list
        elements: int

      machine_type:
        description:
          - The type of device, as reported by the node when added.
        type: list
        elements: str

      node_description:
        description:
          - Summary describing the device hardware and software.
        type: list
        elements: str

      is_server:
        description:
          - Whether node is considered a server. Note that this is only as reliable
            as your SolarWinds data.
        type: bool

      unmanaged:
        description:
          - Whether node is currently managed.
        type: bool

      status:
        description:
          - Node statuses to include, for example "Up", "Down", "Active",
            "Critical", "Off", "Online", "Rebooting", and so on.
          - Note that there are 40 distinct possible values, as per the StatusName
            column in Orion.StatusInfo.
        type: list
        elements: str

      severity_max:
        description:
          - The maximum severity to include.
        type: int

      severity_min:
        description:
          - The minimum severity to include.
        type: int

      vendor:
        description:
          - Vendor name.
        type: list
        elements: str

      agent:
        description:
          - A map whose keys are the names of one or more columns in the
            Orion.AgentManagement.Agent table and one or more values to match for
            each.
        type: dict

      custom_properties:
        description:
          - A map whose keys are the names of one or more columns in the
            Orion.CustomProperty table and one or more values to match for each.
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
      anophelesgreyhoe.solarwinds.orion_node_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        filters:
          snmp_version:
            - 2
            - 3
      delegate_to: localhost

- name: Find all nodes in Australia with IP addresses starting with '10.100.0.'
  hosts: localhost
  gather_facts: no
  tasks:
    - name:  Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.orion_node_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        filters:
          custom_properties:
            Country: Australia
          ip_address:
            - "10.100.0.%"
      delegate_to: localhost

- name: Find all nodes currently having severity of 100 or higher
  hosts: localhost
  gather_facts: no
  tasks:
    - name:  Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.orion_node_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        filters:
          severity_min: 100
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
    Object to retrieve information about nodes in Solarwinds Orion NPM.
    """

    def __init__(self, solarwinds):
        self.solarwinds = solarwinds
        self.changed = False

    def custom_properties(self, module):
        query = "SELECT Field FROM Orion.CustomProperty WHERE TargetEntity = 'Orion.NodesCustomProperties'"
        try:
            query_res = self.solarwinds.client.query(query)
        except Exception as ex:
            module.fail_json(msg="Custom properties query failed: {0}".format(str(ex)))
        if "results" in query_res and query_res["results"]:
            return query_res["results"]
        else:
            return None

    def node_agents(self, module, node_ids):
        if not node_ids:
            return None
        agent_columns = [
            "NodeID",
            "AgentId",
            "Name",
            "Hostname",
            "DNSName",
            "IP",
            "OSVersion",
            "PollingEngineId",
            "ConnectionStatus",
            "ConnectionStatusMessage",
            "ConnectionStatusTimestamp",
            "AgentStatus",
            "AgentStatusMessage",
            "AgentStatusTimestamp",
            "IsActiveAgent",
            "Mode",
            "AgentVersion",
            "AutoUpdateEnabled",
            "OrionIdColumn",
            "PassiveAgentHostname",
            "PassiveAgentPort",
            "ProxyId",
            "RegisteredOn",
            "SID",
            "Is64Windows",
            "CPUArch",
            "OSArch",
            "OSDistro",
            "ResponseTime",
            "Type",
            "RuntimeOSDistro",
            "RuntimeOSVersion",
            "RuntimeOSLabel",
            "OSLabel",
        ]
        agent_projection = ", ".join(agent_columns)
        query = (
            "SELECT {0} FROM Orion.AgentManagement.Agent WHERE NodeID IN ({1})".format(
                agent_projection, ", ".join([str(i) for i in node_ids])
            )
        )
        try:
            query_res = self.solarwinds.client.query(query)
        except Exception as ex:
            module.fail_json(msg="Node agent query failed: {0}".format(str(ex)))
        if "results" in query_res and query_res["results"]:
            return query_res["results"]
        else:
            return None

    def node_custom_properties(self, module, node_ids):
        if not node_ids:
            return None
        custom_properties = self.custom_properties(module)
        custom_property_fields = [f["Field"] for f in custom_properties]
        field_projection = ", ".join(["NodeID"] + custom_property_fields)
        query = (
            "SELECT {0} FROM Orion.NodesCustomProperties WHERE NodeID IN ({1})".format(
                field_projection, ", ".join([str(i) for i in node_ids])
            )
        )
        try:
            query_res = self.solarwinds.client.query(query)
        except Exception as ex:
            module.fail_json(
                msg="Node custom properties query failed: {0}".format(str(ex))
            )
        if "results" in query_res and query_res["results"]:
            return query_res["results"]
        else:
            return None

    def info(self, module):
        query = SolarWindsQuery(module, self.solarwinds.client)
        query.base_table = module.params["base_table"]
        query.input_include = module.params["include"]
        query.input_exclude = module.params["exclude"]
        query.input_columns = module.params["columns"]
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
