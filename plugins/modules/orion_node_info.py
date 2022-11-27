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
    L(SolarWinds Information Service (SWIS),
    https://solarwinds.github.io/OrionSDK/2020.2/schema/Orion.Nodes.html).
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

  unmanaged:
    description:
      - Whether the node is currently managed.
    type: bool

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
        severity_min: 100
      delegate_to: localhost
"""

# TODO: Add Ansible module RETURN section

import traceback

from ansible.module_utils.basic import AnsibleModule, missing_required_lib
from ansible.module_utils._text import to_native
from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.solarwinds_client import (
    SolarwindsClient,
    solarwindsclient_argument_spec,
)

try:
    import requests

    requests.urllib3.disable_warnings()
except ImportError:
    HAS_REQUESTS = False
    REQUESTS_IMPORT_ERROR = traceback.format_exc()
else:
    HAS_REQUESTS = True


class OrionNodeInfo(object):
    """
    Object to retrieve information about nodes in Solarwinds Orion NPM.
    """

    def __init__(self, solarwinds):
        self.solarwinds = solarwinds
        self.module = self.solarwinds.module
        self.client = self.solarwinds.client
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
        # module.fail_json(msg=str(query_res))
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

    def nodes(self, module):
        criteria_arguments = {
            "agent": {
                "table_alias": "a",
                "table": "Orion.AgentManagement.Agent",
            },
            "caption": {
                "table_alias": "n",
                "column": "Caption",
            },
            "custom_properties": {
                "table_alias": "cp",
                "table": "Orion.NodesCustomProperties",
            },
            "dns": {
                "table_alias": "n",
                "column": "DNS",
            },
            "ip_address": {"table_alias": "n", "column": "IPAddress"},
            "is_server": {
                "table_alias": "n",
                "column": "IsServer",
            },
            "machine_type": {
                "table_alias": "n",
                "column": "MachineType",
            },
            "node_description": {
                "table_alias": "n",
                "column": "NodeDescription",
            },
            "node_id": {
                "table_alias": "n",
                "column": "NodeID",
            },
            "object_sub_type": {
                "table_alias": "n",
                "column": "ObjectSubType",
            },
            "polling_engine_id": {
                "table_alias": "n",
                "column": "EngineID",
            },
            "polling_method": {
                "table_alias": "n",
                "column": "ObjectSubType",
            },
            "severity_min": {
                "table_alias": "n",
                "column": "Severity",
                "comparator": ">=",
            },
            "severity_max": {
                "table_alias": "n",
                "column": "Severity",
                "comparator": "<=",
            },
            "snmp_version": {
                "table_alias": "n",
                "column": "SNMPVersion",
            },
            "status": {
                "table_alias": "si",
                "column": "StatusName",
            },
            "sys_name": {
                "table_alias": "n",
                "column": "SysName",
            },
            "unmanaged": {
                "table_alias": "n",
                "column": "Unmanaged",
            },
            "vendor": {
                "table_alias": "n",
                "column": "Vendor",
            },
        }

        base_query = {
            "order": ["Nodes", "StatusInfo"],
            "tables": {
                "Nodes": {
                    "schema": "Orion",
                    "alias": "n",
                    "columns": [
                        "NodeID",
                        "Caption",
                        "DNS",
                        "IPAddress",
                        "IPAddressType",
                        "DynamicIP",
                        "MachineType",
                        "Vendor",
                        "Description",
                        "NodeDescription",
                        "EngineID",
                        "PollInterval",
                        "RediscoveryInterval",
                        "MinutesSinceLastSync",
                        "ObjectSubType",
                        "SNMPVersion",
                        "IsServer",
                        "Severity",
                        "CPUCount",
                        "CPULoad",
                        "MemoryUsed",
                        "LoadAverage1",
                        "LoadAverage5",
                        "LoadAverage15",
                        "MemoryAvailable",
                        "PercentMemoryUsed",
                        "PercentMemoryAvailable",
                        "LastBoot",
                        "SystemUpTime",
                        "Location",
                        "Contact",
                        "Unmanaged",
                        "UnManageFrom",
                        "UnManageUntil",
                        "Uri",
                    ],
                },
                "StatusInfo": {
                    "schema": "Orion",
                    "alias": "si",
                    "join": "INNER JOIN Orion.StatusInfo si ON si.StatusId = n.Status",
                    "columns": [
                        "StatusId",
                        "StatusName",
                        "ShortDescription",
                    ],
                },
            },
        }

        base_columns = []
        for t in base_query["tables"].keys():
            for c in base_query["tables"][t]["columns"]:
                base_columns.append(".".join([base_query["tables"][t]["alias"], c]))

        criteria = ""
        argument_spec = module.argument_spec
        params = module.params
        # Iterate over mappings of params to columns
        for k in criteria_arguments:
            field_criteria = ""
            # Iterate over params for which we have values
            if k in params and (
                params[k]
                or isinstance(params[k], bool)
                or isinstance(params[k], str)
                or isinstance(params[k], int)
            ):
                # Filtering: translate params into a more generic criteria format to simplify querying
                table_alias = criteria_arguments[k]["table_alias"]
                if argument_spec[k]["type"] != "dict":
                    column = criteria_arguments[k]["column"]
                if not isinstance(params[k], list) and not isinstance(params[k], dict):
                    param_value = [params[k]]
                else:
                    param_value = params[k]

                # Iterate over each value for the current param and validate
                for element in param_value:
                    match = None
                    if argument_spec[k]["type"] == "dict":
                        column = element
                        match = param_value[element]
                    elif argument_spec[k]["type"] == "str":
                        if isinstance(element, str):
                            match = element
                    elif argument_spec[k]["type"] == "int":
                        try:
                            match = int(element)
                        except Exception as ex:
                            pass
                    elif argument_spec[k]["type"] == "bool":
                        if isinstance(element, bool):
                            match = str(element)
                        elif isinstance(element, str):
                            if str(element).lower() in ["yes", "on", "true"]:
                                match = "True"
                            elif str(element).lower() in ["no", "off", "false"]:
                                match = "False"
                    else:
                        match = element

                    if not match:
                        module.fail_json(
                            msg="Criterion for field '{0}' should be {1}".format(
                                k, argument_spec[k]["type"]
                            )
                        )

                    # Determine how criteria are used to filter results
                    if argument_spec[k]["type"] in ["dict", "str"] or (
                        "elements" in argument_spec[k]
                        and argument_spec[k]["elements"] == "str"
                    ):
                        comparator = "LIKE"
                        wrap = "'"
                    elif (
                        argument_spec[k]["type"] == "int"
                        and "comparator" in criteria_arguments[k]
                    ):
                        comparator = criteria_arguments[k]["comparator"]
                        wrap = None
                    # This base case should work for everything else, including lists of ints
                    else:
                        comparator = "="
                        wrap = None
                    if wrap:
                        criterion = f"{wrap}{match}{wrap}"
                    else:
                        criterion = str(match)
                    if field_criteria:
                        field_criteria = " ".join([field_criteria, "OR"])
                    field_criteria = " ".join(
                        [
                            field_criteria,
                            ".".join([table_alias, column]),
                            comparator,
                            criterion,
                        ]
                    )
                if criteria:
                    criteria = " ".join([criteria, "AND "])
                criteria += "(" + field_criteria.strip() + ")"

        # Generate other parts of the SWIS query
        projection = ", ".join(base_columns)
        tables_spec = []
        for t in base_query["tables"].keys():
            if "join" in base_query["tables"][t] and base_query["tables"][t]["join"]:
                table_ref = base_query["tables"][t]["join"]
            else:
                table_ref = " ".join(
                    [
                        ".".join([base_query["tables"][t]["schema"], t]),
                        base_query["tables"][t]["alias"],
                    ]
                )
            tables_spec.append(table_ref)

        if params["custom_properties"]:
            tables_spec.append(
                "INNER JOIN Orion.NodesCustomProperties cp ON cp.NodeID = n.NodeID"
            )
        if params["agent"]:
            tables_spec.append(
                "INNER JOIN Orion.AgentManagement.Agent a ON a.NodeID = n.NodeID"
            )
        from_clause = " ".join(["FROM", " ".join(tables_spec)])
        where_clause = " ".join(["WHERE", criteria])

        # Assemble and run the query
        query = " ".join(["SELECT", projection, from_clause, where_clause])

        try:
            query_res = self.solarwinds.client.query(query)
        except Exception as ex:
            module.fail_json(msg="Nodes query failed: {0}".format(str(ex)))

        if "results" in query_res:
            query_results = query_res["results"]
            node_ids = [i["NodeID"] for i in query_results]
            node_custom_properties = self.node_custom_properties(module, node_ids)
            if node_custom_properties is not None:
                node_custom_properties_indexed = {
                    ncp["NodeID"]: ncp for ncp in node_custom_properties
                }
            else:
                node_custom_properties_indexed = {}
            node_agents = self.node_agents(module, node_ids)
            if node_agents is not None:
                node_agents_indexed = {na["NodeID"]: na for na in node_agents}
            else:
                node_agents_indexed = {}

            results = []
            for node_data in query_results:
                if node_data["NodeID"] in node_custom_properties_indexed:
                    node_data.update(
                        {
                            "CustomProperties": node_custom_properties_indexed[
                                node_data["NodeID"]
                            ]
                        }
                    )
                if node_data["NodeID"] in node_agents_indexed:
                    node_data.update(
                        {"Agent": node_agents_indexed[node_data["NodeID"]]}
                    )

                results.append(node_data)

        else:
            results = None
        return results


# ==============================================================
# main


def main():

    argument_spec = dict(
        agent=dict(type="dict", default={}),
        caption=dict(type="list", elements="str", default=[]),
        custom_properties=dict(type="dict", default={}),
        dns=dict(type="list", elements="str", default=[]),
        ip_address=dict(type="list", elements="str", default=[]),
        is_server=dict(type="bool"),
        machine_type=dict(type="list", elements="str", default=[]),
        node_description=dict(type="list", elements="str", default=[]),
        node_id=dict(type="list", elements="int", default=[]),
        object_sub_type=dict(
            type="list",
            elements="str",
            choices=["agent", "icmp", "snmp", "wmi"],
            default=[],
        ),
        polling_engine_id=dict(type="list", elements="int", default=[]),
        polling_method=dict(
            type="list",
            elements="str",
            choices=["agent", "icmp", "snmp", "wmi"],
            default=[],
        ),
        severity_max=dict(type="int"),
        severity_min=dict(type="int"),
        snmp_version=dict(
            type="list", elements="int", choices=[0, 1, 2, 3], default=[]
        ),
        status=dict(type="list", elements="str", default=[]),
        sys_name=dict(type="list", elements="str", default=[]),
        unmanaged=dict(type="bool"),
        vendor=dict(type="list", elements="str", default=[]),
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

    solarwinds = SolarwindsClient(module)
    orion_node_info = OrionNodeInfo(solarwinds)

    results = orion_node_info.nodes(module)
    res_args = dict(changed=False, results=results)

    module.exit_json(**res_args)


if __name__ == "__main__":
    main()
