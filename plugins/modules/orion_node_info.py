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
  - Retrieve information about nodes in SolarWinds Orion NPM.

extends_documentation_fragment:
  - anophelesgreyhoe.solarwinds.solarwinds_client

version_added: "2.0.0"

author:
  - "Ashley Hooper (@ashleyghooper)"

options:
  query:
    description:
      - A raw SWIS query, e.g. "SELECT DNS WHERE IPAddress LIKE '10.137.150.%'"
    type: str
    choices: [ "node", "swis" ]  # TODO: add other entities to query

  swis_query:
    description:
      - A raw SWIS query, e.g. "SELECT DNS WHERE IPAddress LIKE '10.137.150.%'"
    type: str

  node_id:
    description:
      - node_id of the node.
      - One of 'node_id', 'node_name', or 'ip_address' must be provided.
    type: str

  node_name:
    description:
      - FQDN of the node.
      - For adding a node this field is required.
      - For all other states field is optional.
    type: str

  caption:
    description:
      - The SolarWinds 'caption' for the node, if different from the node name.
    type: str

  ip_address:
    description:
      - IP Address of the node.
      - One of 'node_id', 'node_name', or 'ip_address' must be provided.
    type: str

  unmanage_from:
    description:
      - "The date and time (in ISO 8601 UTC format) to begin the unmanage period."
      - If this is in the past, the node will be unmanaged effective immediately.
      - If not provided, module defaults to now.
      - "ex: 2017-02-21T12:00:00Z"
    type: str

  unmanage_until:
    description:
      - "The date and time (in ISO 8601 UTC format) to end the unmanage period."
      - You can set this as far in the future as you like.
      - If not provided, module defaults to 24 hours from now.
      - "ex: 2017-02-21T12:00:00Z"
    type: str

  polling_method:
    description:
      - Polling method to use.
    choices: [ "agent", "external", "icmp", "snmp", "wmi" ]
    type: str

  agent_mode:
    description:
      - Mode of communication between polling engine and agent - either 'active' for agent-initiated communication, or 'passive' for server-initiated.
    choices:
      - active
      - passive
    type: str

  agent_port:
    description:
      - Port used to communicate with the agent.
    type: int
    default: 17790

  agent_auto_update:
    description:
      - Enable automatic update of agent versions.
    type: bool
    default: 'no'

  polling_engine_id:
    description:
      - Id of polling engine to move the node to after successful discovery.
    type: int

  polling_engine_name:
    description:
      - Name of polling engine to move the node to after successful discovery.
    type: str

  snmp_version:
    description:
      - SNMPv2c is used by default.
      - SNMPv3 requires use of existing, named SNMPv3 credentials within Orion.
    choices: [ "2c", "3" ]
    type: str
    default: "2c"

  snmp_port:
    description:
      - port that SNMP server listens on
    type: int
    default: 161

  snmp_allow_64:
    description:
      - Set true if device supports 64-bit counters
    type: bool
    default: true

  credential_names:
    description:
      - List of credential names to use for node discovery
    type: list
    elements: str

  custom_properties:
    description:
      - A dictionary containing custom properties and their values
    type: dict

requirements:
  - "python >= 2.6"
  - dateutil
  - requests
  - traceback
"""

EXAMPLES = r"""
- name: Find all systems
  hosts: localhost
  gather_facts: no
  tasks:
    - name:  Run a regular SolarWinds Information Service query
      anophelesgreyhoe.solarwinds.orion_node_info:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        query: "SELECT DNS WHERE IPAddress LIKE '10.137.150.%'"
      delegate_to: localhost
      throttle: 1
"""

# TODO: Add Ansible module RETURN section

from datetime import datetime, timedelta
import re
import time
import traceback

from ansible.module_utils.basic import AnsibleModule, missing_required_lib
from ansible.module_utils._text import to_native
from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.solarwinds_client import (
    SolarwindsClient,
    solarwindsclient_argument_spec,
)

# Basic UTC timezone for python2.7 compatibility
from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.utc import UTC

try:
    from dateutil.parser import parse
except ImportError:
    HAS_DATEUTIL = False
    DATEUTIL_IMPORT_ERROR = traceback.format_exc()
else:
    HAS_DATEUTIL = True

try:
    import requests

    requests.urllib3.disable_warnings()
except ImportError:
    HAS_REQUESTS = False
    REQUESTS_IMPORT_ERROR = traceback.format_exc()
else:
    HAS_REQUESTS = True


# These constants control how many times and at what interval this module
# will check the status of the Orion ListResources job to see if it has completed.
# Total time will be retries multiplied by sleep seconds.
LIST_RESOURCES_STATUS_CHECK_RETRIES = 60
LIST_RESOURCES_RETRY_SLEEP_SECS = 3

# Other Orion constants for numeric fields
ORION_CONN_STATUS_CONNECTED = 1


class OrionNodeInfo(object):
    """
    Object to retrieve information about nodes in Solarwinds Orion NPM
    """

    def __init__(self, solarwinds):
        self.solarwinds = solarwinds
        self.module = self.solarwinds.module
        self.client = self.solarwinds.client
        self.utc = UTC()
        self.changed = False

    # This method already exists in orion_node.py - if required here, perhaps move to solarwinds_client.py
    # def agent(self, module):
    #     return None

    def nodes(self, module):
        criteria_arguments = {
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
            "node_id": {
                "table_alias": "n",
                "column": "NodeID",
            },
            "object_sub_type": {
                "table_alias": "n",
                "column": "ObjectSubType",
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

        node_base_fields = [
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
            "ObjectSubType",
            "SNMPVersion",
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
        ]
        status_info_base_fields = [
            "StatusId",
            "StatusName",
            "ShortDescription",
        ]
        base_fields = ["n." + f for f in node_base_fields] + [
            "si." + f for f in status_info_base_fields
        ]
        extra_fields = []
        # return dict(changed=False, msg=module.argument_spec)

        criteria = ""
        argument_spec = module.argument_spec
        params = module.params
        for k in criteria_arguments:
            field_criteria = ""
            if k in params and (
                params[k] or isinstance(params[k], bool or isinstance(params[k], str))
            ):
                table_alias = criteria_arguments[k]["table_alias"]
                if argument_spec[k]["type"] != "dict":
                    column = criteria_arguments[k]["column"]
                if not isinstance(params[k], list) and not isinstance(params[k], dict):
                    param_value = [params[k]]
                else:
                    param_value = params[k]
                for element in param_value:
                    if argument_spec[k]["type"] == "dict":
                        column = element
                        match = param_value[element]
                    elif argument_spec[k]["type"] == "bool":
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
                                msg="Field '{0}' should be boolean: {1}".format(
                                    k, str(ex)
                                )
                            )
                    elif argument_spec[k]["type"] == "str":
                        if isinstance(element, str):
                            match = str(element)
                        else:
                            module.fail_json(
                                msg="Field '{0}' should be string: {1}".format(
                                    k, str(ex)
                                )
                            )
                    else:
                        match = element
                    if argument_spec[k]["type"] in ["dict", "str"] or (
                        "elements" in argument_spec[k]
                        and argument_spec[k]["elements"] == "str"
                    ):
                        comparator = "LIKE"
                        wrap = "'"
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
                extra_field = ".".join([table_alias, column])
                if extra_field not in base_fields + extra_fields:
                    extra_fields.append(extra_field)

        projection = ", ".join(base_fields + extra_fields)
        tables = ["Orion.Nodes n"]
        tables.append("INNER JOIN Orion.StatusInfo si ON si.StatusId = n.Status")
        if params["custom_properties"]:
            tables.append(
                "INNER JOIN Orion.NodesCustomProperties cp ON cp.NodeID = n.NodeID"
            )
        from_clause = " ".join(["FROM", " ".join(tables)])
        where_clause = " ".join(["WHERE", criteria])

        query = " ".join(["SELECT", projection, from_clause, where_clause])

        # return dict(changed=False, msg=query)

        try:
            query_res = self.solarwinds.client.query(query)
        except Exception as ex:
            module.fail_json(msg="Query failed: {0}".format(str(ex)))
        if "results" in query_res:
            results = query_res["results"]
        else:
            results = None
        return results


# ==============================================================
# main


def main():

    argument_spec = dict(
        # query=dict(choices=["node", "swis"], required=True),
        # swis_query=dict(type="str"),
        caption=dict(type="list", elements="str", default=[]),
        custom_properties=dict(type="dict", default={}),
        dns=dict(type="list", elements="str", default=[]),
        ip_address=dict(type="list", elements="str", default=[]),
        node_id=dict(type="list", elements="int", default=[]),
        object_sub_type=dict(type="list", elements="str", default=[]),
        snmp_version=dict(type="list", elements="str", default=[]),
        sys_name=dict(type="list", elements="str", default=[]),
        status=dict(type="str"),
        unmanaged=dict(type="bool"),
        vendor=dict(type="str"),
        # max_items=dict(type="int"),
        # node_id=dict(type="str"),
        # ip_address=dict(type="str"),
        # node_name=dict(type="str"),
        # caption=dict(type="str"),
        # unmanage_from=dict(type="str", default=None),
        # unmanage_until=dict(type="str", default=None),
        # polling_method=dict(
        #     type="str", choices=["agent", "external", "icmp", "snmp", "wmi"]
        # ),
        # agent_mode=dict(type="str", choices=["active", "passive"]),
        # agent_port=dict(type="int", default=17790),
        # agent_shared_secret=dict(type="str", no_log=True),
        # agent_auto_update=dict(type="bool", default=False),
        # polling_engine_id=dict(type="int"),
        # polling_engine_name=dict(type="str"),
        # discovery_polling_engine_name=dict(type="str"),
        # snmp_version=dict(type="str", default="2c", choices=["2c", "3"]),
        # snmp_port=dict(type="int", default=161),
        # snmp_allow_64=dict(type="bool", default=True),
        # credential_names=dict(type="list", elements="str", default=[]),
        # discovery_interface_filters=dict(type="list", elements="dict", default=[]),
        # interface_filters=dict(type="list", elements="dict", default=[]),
        # interface_filter_cutoff_max=dict(type="int", default=10),
        # volume_filters=dict(type="list", elements="dict", default=[]),
        # volume_filter_cutoff_max=dict(type="int", default=50),
        # custom_properties=dict(type="dict", default={}),
    )

    argument_spec.update(solarwindsclient_argument_spec())

    module = AnsibleModule(
        argument_spec=argument_spec,
        # mutually_exclusive=[["polling_engine_id", "polling_engine_name"]],
        # required_together=[],
        # required_one_of=[["query"]],
        required_if=[
            #     # ["state", "present", ["polling_method"]],  # TODO: reinstate this once orion_node_facts
            #     ["state", "muted", ["unmanage_from", "unmanage_until"]],
            #     ["state", "remanaged", ["unmanage_until"]],
            #     ["state", "unmanaged", ["unmanage_from", "unmanage_until"]],
            #     ["state", "unmuted", ["unmanage_until"]],
            #     [
            #         "polling_method",
            #         "agent",
            #         ["agent_mode", "agent_port", "agent_auto_update"],
            #     ],
            #     [
            #         "polling_method",
            #         "snmp",
            #         ["credential_names", "snmp_version", "snmp_port", "snmp_allow_64"],
            #     ],
            #     ["polling_method", "wmi", ["credential_names"]],
            #     ["agent_mode", "passive", ["agent_port", "agent_shared_secret"]],
        ],
        supports_check_mode=True,
    )

    if not HAS_DATEUTIL:
        module.fail_json(
            msg=missing_required_lib("dateutil"), exception=DATEUTIL_IMPORT_ERROR
        )
    if not HAS_REQUESTS:
        module.fail_json(
            msg=missing_required_lib("requests"), exception=REQUESTS_IMPORT_ERROR
        )

    solarwinds = SolarwindsClient(module)
    orion_node_info = OrionNodeInfo(solarwinds)

    # res_args = orion_node_info.nodes(module)

    results = orion_node_info.nodes(module)
    res_args = dict(changed=False, results=results)

    # if module.params["ip_address"]:
    #     res_args = orion_node_info.nodes()
    #     try:
    #         query_res = solarwinds.client.query(module.params["swis_query"])
    #     except Exception as ex:
    #         module.fail_json(msg="Query failed: {0}".format(str(ex)))
    #     if "results" in query_res:
    #         results = query_res["results"]
    #     else:
    #         results = None
    #     res_args = dict(changed=False, results=results)

    # elif module.params["ip_address"]:
    #     pass

    module.exit_json(**res_args)


if __name__ == "__main__":
    main()
