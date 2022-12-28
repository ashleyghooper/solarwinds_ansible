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
    SolarwindsClient,
    solarwindsclient_argument_spec,
    SolarwindsQuery,
)

# TODO: Delete
# from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.model import (
#     Model,
# )

from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.query_builder import (
    QueryBuilder,
)

# TODO: Delete
# from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.swis_query_nodes import (
#     SwisQueryNodes,
# )

REQUESTS_IMPORT_ERROR = None
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
        base_table = "Orion.Nodes"
        query = SolarwindsQuery(module, self.solarwinds.client, base_table)
        query.input_filters = module.params["filters"]
        query.input_columns = module.params["columns"]
        query_res = query.execute()

        module.fail_json(msg="{0}".format(str(query_metadata["projected_columns"])))

        # filters = {}
        # if "filters" in module.params and module.params["filters"] != {}:
        #     filters = module.params["filters"]
        #     extra_tables_filtering = [
        #         t
        #         for t in module.params["filters"].keys()
        #         if t.lower() != base_table.lower()
        #     ]

        # # module.fail_json(msg="{0}".format(str(filters)))

        # if "columns" in module.params and module.params["columns"] != []:
        #     extra_tables_projecting = [
        #         t
        #         for t in module.params["columns"].keys()
        #         if t.lower() != base_table.lower()
        #     ]

        # extra_tables = list(set(extra_tables_filtering) | set(extra_tables_projecting))
        # query_tables = [base_table] + extra_tables

        # module.fail_json(msg="{0}".format(str(query_tables)))

        # properties = self.solarwinds.properties(module, query_tables)
        # module.fail_json(msg="Properties: {0}".format(str(properties)))

        # relationships = self.solarwinds.relationships(
        #     module,
        #     base_table,
        #     [t for t in properties if t.lower() != base_table.lower()],
        # )

        # module.fail_json(msg="Properties: {0}".format(str(relationships)))

        # Verify all projected columns and columns used for filtering are valid
        projected_columns = [
            ".".join([entity_data["aliases"][t], entity_data["properties"][t][p]])
            for t in entity_data["all_tables"]
            for p in list(entity_data["properties"][t].keys())
        ]

        module.fail_json(msg="Properties: {0}".format(str(projected_columns)))

        projected_columns = {}
        for table in entity_data["all_tables"]:
            columns = []
            for property in entity_data["properties"][table]:

                # for property in list(
                #     set(
                #         module.params["columns"][table]
                #         if table in module.params["columns"]
                #         else []
                #     )
                #     | set(
                #         [t for t in module.params["filters"][table]]
                #         if table in module.params["filters"]
                #         else []
                #     )
                # ):
                if property.lower() in [p.lower() for p in properties[table]]:
                    columns.append(
                        ".".join(
                            [
                                "".join(
                                    [u for u in properties[table] if u.isupper()]
                                ).lower(),
                                property,
                            ]
                        )
                    )
                else:
                    module.fail_json(
                        msg="Property '{0}' was not found in table '{1}'".format(
                            property, table
                        )
                    )
            projected_columns[table] = columns

        # model = Model(self.solarwinds, module.params["columns"])
        # query_columns = model.query_columns()

        # module.fail_json(msg=str(projected_columns[base_table]))

        query = (
            QueryBuilder()
            .SELECT(*projected_columns[base_table])
            .FROM("Orion.Nodes AS n")
        )

        module.fail_json(msg="Query = {0}".format(str(query)))

        if filters:
            for filter_table in filters:
                for filter_property in filter_table:
                    if table != "Nodes":
                        left, right = table_class.joins["Nodes"]
                        query.INNER_JOIN(
                            "{0}.{1} AS {2} ON {3}.{4} = {5}.{6}".format(
                                table_class.schema,
                                table,
                                alias,
                                alias,
                                left,
                                "n",
                                right,
                            )
                        )
                    table_filters = module.params["filters"][table]
                    for column in [
                        f for f in table_filters if f is not None and f != []
                    ]:
                        criteria = table_filters[column]
                        # Filtering: translate params into a more generic criteria format to simplify querying
                        column_criteria = ""
                        if not isinstance(criteria, list) and not isinstance(
                            criteria, dict
                        ):
                            param_value = [criteria]
                        else:
                            param_value = criteria

                        # Iterate over each value for the current param and validate
                        for element in [v for v in param_value if v is not None]:
                            match = None
                            comparator = "LIKE"
                            wrap = "'"
                            if (
                                hasattr(table_class, "boolean_columns")
                                and column in table_class.boolean_columns
                            ):
                                comparator = "="
                                if isinstance(element, bool):
                                    match = str(element)
                                elif isinstance(element, str):
                                    match = (
                                        "True"
                                        if str(element).lower() in ["yes", "on", "true"]
                                        else "False"
                                    )
                                else:
                                    module.fail_json(
                                        msg="All filters on column '{0}' should be boolean: {1}".format(
                                            column
                                        )
                                    )
                            elif isinstance(criteria, dict):
                                column = element
                                match = param_value[element]
                            elif isinstance(criteria, str):
                                match = element
                            elif isinstance(criteria, int):
                                wrap = None
                                try:
                                    match = int(element)
                                except Exception as ex:
                                    pass
                            else:
                                match = element

                            if not match:
                                module.fail_json(
                                    msg="Invalid filter for column '{0}'".format(column)
                                )

                            if wrap:
                                criterion = "{0}{1}{2}".format(wrap, match, wrap)
                            else:
                                criterion = str(match)

                            if column_criteria:
                                column_criteria = " ".join([column_criteria, "OR"])
                            column_criteria += " ".join(
                                [
                                    ".".join([alias, column]),
                                    comparator,
                                    criterion,
                                ]
                            )

                            query.WHERE("({0})".format(column_criteria))

        # module.fail_json(msg="{0}".format(str(query)))

        # from_where = [" ".join(["FROM", " ".join(tables_spec)])]
        # if len(criteria.strip()) > 0:
        #     from_where.append(" ".join(["WHERE", criteria]))

        # Assemble and run the query
        # query = " ".join(["SELECT", projection] + from_where)

        try:
            query_res = self.solarwinds.client.query(str(query))
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

            nodes = []
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

                nodes.append(node_data)

        else:
            nodes = None
        info = {"nodes": nodes, "count": len(nodes), "query": str(query)}
        return info


# ==============================================================
# main


def main():

    argument_spec = dict(
        columns=dict(
            type="dict",
        ),
        #     options=dict(
        #         Nodes=dict(
        #             type="list",
        #             default=["NodeID", "Caption", "DNS", "IPAddress", "Uri"],
        #         ),
        #         Agents=dict(type="list", default=[]),
        #         NodesCustomProperties=dict(type="list", default=[]),
        #     ),
        # ),
        filters=dict(
            type="dict",
        ),
        # options=dict(
        #     Nodes=dict(
        #         type="dict",
        #         options=dict(
        #             Caption=dict(type="list", elements="str", default=[]),
        #             DNS=dict(type="list", elements="str", default=[]),
        #             IPAddress=dict(type="list", elements="str", default=[]),
        #             IsServer=dict(type="bool"),
        #             MachineType=dict(type="list", elements="str", default=[]),
        #             NodeDescription=dict(type="list", elements="str", default=[]),
        #             NodeID=dict(type="list", elements="int", default=[]),
        #             ObjectSubType=dict(
        #                 type="list",
        #                 elements="str",
        #                 choices=["Agent", "ICMP", "SNMP", "WMI"],
        #                 default=[],
        #             ),
        #             EngineID=dict(type="list", elements="int", default=[]),
        #             SNMPVersion=dict(
        #                 type="list",
        #                 elements="int",
        #                 choices=[0, 1, 2, 3],
        #                 default=[],
        #             ),
        #         ),
        #     ),
        #     Agents=dict(type="dict", default={}),
        #     NodesCustomProperties=dict(type="dict", default={})
        # agent=dict(type="dict", default={}),
        # caption=dict(type="list", elements="str", default=[]),
        # custom_properties=dict(type="dict", default={}),
        # dns=dict(type="list", elements="str", default=[]),
        # ip_address=dict(type="list", elements="str", default=[]),
        # is_server=dict(type="bool"),
        # machine_type=dict(type="list", elements="str", default=[]),
        # node_description=dict(type="list", elements="str", default=[]),
        # node_id=dict(type="list", elements="int", default=[]),
        # object_sub_type=dict(
        #     type="list",
        #     elements="str",
        #     choices=["agent", "icmp", "snmp", "wmi"],
        #     default=[],
        # ),
        # polling_engine_id=dict(type="list", elements="int", default=[]),
        # polling_method=dict(
        #     type="list",
        #     elements="str",
        #     choices=["agent", "icmp", "snmp", "wmi"],
        #     default=[],
        # ),
        # severity_max=dict(type="int"),
        # severity_min=dict(type="int"),
        # snmp_version=dict(
        #     type="list", elements="int", choices=[0, 1, 2, 3], default=[]
        # ),
        # status=dict(type="list", elements="str", default=[]),
        # sys_name=dict(type="list", elements="str", default=[]),
        # unmanaged=dict(type="bool"),
        # vendor=dict(type="list", elements="str", default=[]),
        # ),
        # ),
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

    res_args = dict(changed=False, orion_node_info=orion_node_info.nodes(module))

    module.exit_json(**res_args)


if __name__ == "__main__":
    main()
