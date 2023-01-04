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
module: orion_node

short_description: Create/remove/modify nodes in SolarWinds Orion NPM

description:
  - Manage nodes in SolarWinds Orion NPM.

extends_documentation_fragment:
  - anophelesgreyhoe.solarwinds.solarwinds_client

version_added: "1.0.0"

author:
  - "Jarett D Chaiken (@jdchaiken)"
  - "Ashley Hooper (@ashleyghooper)"

options:
  state:
    description:
      - The desired state of the node.
    choices: ['present', 'absent', 'remanaged', 'unmanaged', 'muted', 'unmuted']
    type: str
    default: "remanaged"

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

  agent_shared_secret:
    description:
      - Secure string used to communicate with the agent.
    type: str

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

  discovery_polling_engine_name:
    description:
      - Name of polling engine that NPM will use for discovery only (only required if different to polling_engine_name).
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

  discovery_interface_filters:
    description:
      - List of SolarWinds Orion interface discovery filters
    type: list
    elements: dict

  interface_filters:
    description:
      - List of regular expressions by which to exclude interfaces from monitoring
    type: list
    elements: dict

  interface_filter_cutoff_max:
    description:
      - Maximum number of interfaces that can be removed from monitoring for a newly discovered device
    type: int
    default: 10

  volume_filters:
    description:
      - List of regular expressions by which to exclude volumes from monitoring
    type: list
    elements: dict

  volume_filter_cutoff_max:
    description:
      - Maximum number of volumes that can be removed from monitoring for a newly discovered device
    type: int
    default: 50

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
- name: Remove nodes
  hosts: all
  gather_facts: no
  tasks:
    - name:  Remove a node from Orion
      anophelesgreyhoe.solarwinds.orion_node:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        node_name: servername
        state: absent
      delegate_to: localhost
      throttle: 1

- name: Mute nodes
  hosts: all
  gather_facts: no
  tasks:
    - anophelesgreyhoe.solarwinds.orion_node:
        solarwinds_connection:
          hostname: "{{ solarwinds_host }}"
          username: "{{ solarwinds_username }}"
          password: "{{ solarwinds_password }}"
        node_name: "{{ inventory_hostname }}"
        state: muted
        unmanage_from: "2020-03-13T20:58:22.033"
        unmanage_until: "2020-03-14T20:58:22.033"
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
    SolarWindsClient,
    solarwindsclient_argument_spec,
)

# Basic UTC timezone for python2.7 compatibility
from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.utc import UTC

DATEUTIL_IMPORT_ERROR = None
try:
    from dateutil.parser import parse
except ImportError:
    HAS_DATEUTIL = False
    DATEUTIL_IMPORT_ERROR = traceback.format_exc()
else:
    HAS_DATEUTIL = True

REQUESTS_IMPORT_ERROR = None
try:
    import requests

    requests.urllib3.disable_warnings()
except ImportError:
    HAS_REQUESTS = False
    REQUESTS_IMPORT_ERROR = traceback.format_exc()
else:
    HAS_REQUESTS = True


# These control checks for node creation after an agent is registered.
AGENT_NODE_CREATION_CHECK_RETRIES = 10
AGENT_NODE_CREATION_SLEEP_SECS = 3
# These constants control how many times and at what interval this module
# will check the status of the Orion discovery job to see if it has completed.
# Total time will be retries multiplied by sleep seconds.
DISCOVERY_STATUS_CHECK_RETRIES = 60
DISCOVERY_RETRY_SLEEP_SECS = 3
# These constants control how many times and at what interval this module
# will check the status of the Orion ListResources job to see if it has completed.
# Total time will be retries multiplied by sleep seconds.
LIST_RESOURCES_STATUS_CHECK_RETRIES = 60
LIST_RESOURCES_RETRY_SLEEP_SECS = 3
# Discovery job statuses
# https://github.com/solarwinds/OrionSDK/blob/master/Samples/PowerShell/DiscoverSnmpV3Node.ps1
ORION_DISCOVERY_JOB_STATUS_UNKNOWN = 0
ORION_DISCOVERY_JOB_STATUS_IN_PROGRESS = 1
ORION_DISCOVERY_JOB_STATUS_FINISHED = 2
ORION_DISCOVERY_JOB_STATUS_ERROR = 3
ORION_DISCOVERY_JOB_STATUS_NOT_SCHEDULED = 4
ORION_DISCOVERY_JOB_STATUS_SCHEDULED = 5
ORION_DISCOVERY_JOB_STATUS_NOT_COMPLETED = 6
ORION_DISCOVERY_JOB_STATUS_CANCELING = 7
ORION_DISCOVERY_JOB_STATUS_READY_FOR_IMPORT = 8

# These control the discovery timeouts within Orion itself.
ORION_DISCOVERY_JOB_TIMEOUT_SECS = 300
ORION_DISCOVERY_SEARCH_TIMEOUT_MS = 20000
ORION_DISCOVERY_SNMP_TIMEOUT_MS = 30000
ORION_DISCOVERY_SNMP_RETRIES = 2
ORION_DISCOVERY_REPEAT_INTERVAL_MS = 3000
ORION_DISCOVERY_WMI_RETRIES_COUNT = 2
ORION_DISCOVERY_WMI_RETRY_INTERVAL_MS = 2000
# Other Orion constants for numeric fields
ORION_CONN_STATUS_CONNECTED = 1

POLLING_METHODS_USING_DISCOVERY = ["snmp", "wmi"]


class OrionNode(object):
    """
    Object to manage nodes in SolarWinds Orion.
    """

    def __init__(self, solarwinds):
        self.solarwinds = solarwinds
        self.module = self.solarwinds.module
        self.client = self.solarwinds.client
        self.utc = UTC()
        self.changed = False

    def agent(self, module):
        agent = None
        results = None
        params = module.params
        base_query_agent = "SELECT AgentId, Name, Hostname, DNSName, IP, NodeId, Uri, ConnectionStatus FROM Orion.AgentManagement.Agent"
        if params["ip_address"] is not None:
            results = self.client.query(
                " ".join(
                    [
                        base_query_agent,
                        "WHERE IP = @ip_address",
                    ]
                ),
                ip_address=params["ip_address"],
            )
        elif params["node_name"] is not None:
            results = self.client.query(
                " ".join(
                    [
                        base_query_agent,
                        "WHERE Name = @agent_name OR Hostname = @agent_name OR DNSName = @agent_name",
                    ]
                ),
                agent_name=params["node_name"],
            )
        else:
            # No Id provided
            module.fail_json(msg="You must provide either ip_address or node_name")

        if results is not None:
            if "results" in results and results["results"]:
                rec = results["results"][0]
                agent = {
                    "agent_id": rec["AgentId"],
                    "name": rec["Name"],
                    "hostname": rec["Hostname"],
                    "dns_name": rec["DNSName"],
                    "uri": rec["Uri"],
                    "connection_status": rec["ConnectionStatus"],
                }
        return agent

    def node(self, module):
        node = None
        results = None
        params = module.params
        base_query_node = " ".join(
            [
                "SELECT OrionIDPrefix, NodeID, Caption, DNS, IPAddress,",
                "IPAddressType, Description, NodeDescription, MachineType,",
                "StatusDescription, SystemUpTime, Vendor, Location, Contact,",
                "ObjectSubType, SNMPVersion, Uri, EngineID,",
                "Unmanaged, UnManageFrom, UnManageUntil",
                "FROM Orion.Nodes",
            ]
        )
        if params["node_id"] is not None:
            results = self.client.query(
                " ".join([base_query_node, "WHERE NodeID = @node_id"]),
                node_id=params["node_id"],
            )
        elif params["ip_address"] is not None:
            results = self.client.query(
                " ".join([base_query_node, "WHERE IPAddress = @ip_address"]),
                ip_address=params["ip_address"],
            )
        elif params["node_name"] is not None:
            results = self.client.query(
                " ".join(
                    [base_query_node, "WHERE Caption = @node_name OR DNS = @node_name"]
                ),
                node_name=params["node_name"],
            )
        else:
            # No Id provided
            self.module.fail_json(
                msg="You must provide either node_id, ip_address, or node_name"
            )

        if results is not None:
            if "results" in results and results["results"]:
                rec = results["results"][0]
                node = {
                    "node_id": rec["NodeID"],
                    "caption": rec["Caption"],
                    "dns_name": rec["DNS"],
                    "ip_address": rec["IPAddress"],
                    "ip_address_type": rec["IPAddressType"],
                    "description": rec["Description"],
                    "node_description": rec["NodeDescription"],
                    "machine_type": rec["MachineType"],
                    "status_description": rec["StatusDescription"],
                    "system_up_time": rec["SystemUpTime"],
                    "vendor": rec["Vendor"],
                    "location": rec["Location"],
                    "contact": rec["Contact"],
                    "object_sub_type": rec["ObjectSubType"],
                    "snmp_version": rec["SNMPVersion"],
                    "netobject_id": "{0}{1}".format(
                        rec["OrionIDPrefix"], rec["NodeID"]
                    ),
                    "uri": rec["Uri"],
                    "engine_id": rec["EngineID"],
                    "unmanaged": rec["Unmanaged"],
                    "unmanage_from": parse(rec["UnManageFrom"]).isoformat(),
                    "unmanage_until": parse(rec["UnManageUntil"]).isoformat(),
                }
        return node

    def validate_fields(self, module):
        # TODO: Get rid of this function and use the Ansible arguments
        # validation options and use another means to map arguments to
        # SolarWinds field names.
        params = module.params
        # Setup properties for new node
        polling_method = params["polling_method"]
        if polling_method == "snmp":
            object_sub_type = "SNMP"
        elif polling_method in ["external", "icmp"]:
            object_sub_type = "ICMP"
        elif polling_method == "agent":
            object_sub_type = "AGENT"
        elif polling_method == "wmi":
            object_sub_type = "WMI"
        else:
            module.fail_json(msg="Polling method not supported")
        props = {
            "ObjectSubType": object_sub_type,
            "External": True if params["polling_method"] == "external" else False,
            "Caption": params["caption"]
            if "caption" in params
            else (
                params["node_name"] if "node_name" in params else params["ip_address"]
            ),
        }

        if "ip_address" in params and params["ip_address"]:
            props["IPAddress"] = params["ip_address"]

        if "." in params["node_name"]:
            props["DNS"] = params["node_name"]

        if params["polling_engine_name"]:
            polling_engine_name = params["polling_engine_name"]
            polling_engine = self.solarwinds.polling_engine(
                module, params["polling_engine_name"]
            )
            if not polling_engine:
                module.fail_json(
                    msg="Failed to find polling engine '{0}'".format(
                        polling_engine_name
                    )
                )
            props["EngineID"] = self.solarwinds.polling_engine(
                module, params["polling_engine_name"]
            )["EngineID"]
        else:
            props["EngineID"] = 1

        # Only set DiscoveryEngineID for polling methods that use discovery
        if params["polling_method"] in POLLING_METHODS_USING_DISCOVERY:
            if (
                "discovery_polling_engine_name" in params
                and params["discovery_polling_engine_name"]
                and params["discovery_polling_engine_name"]
                != params["polling_engine_name"]
            ):
                discovery_polling_engine_name = params["discovery_polling_engine_name"]
                discovery_polling_engine = self.solarwinds.polling_engine(
                    module, params["discovery_polling_engine_name"]
                )
                if not discovery_polling_engine:
                    module.fail_json(
                        msg="Failed to find discovery polling engine '{0}'".format(
                            discovery_polling_engine_name
                        )
                    )
                props["DiscoveryEngineID"] = self.solarwinds.polling_engine(
                    module, params["discovery_polling_engine_name"]
                )["EngineID"]
            else:
                props["DiscoveryEngineID"] = props["EngineID"]

        # Validate required fields
        if not props["IPAddress"]:
            module.fail_json(msg="IP Address is required")

        if not props["External"]:
            if "node_name" not in params:
                module.fail_json(msg="Node name is required")

        if props["ObjectSubType"] == "SNMP":
            props["SNMPVersion"] = params["snmp_version"]
            props["SNMPPort"] = params["snmp_port"]
            props["Allow64BitCounters"] = params["snmp_allow_64"]
            if "SNMPVersion" not in props:
                props["SNMPVersion"] = "2"
            if "SNMPPort" not in props:
                props["SNMPPort"] = "161"
            if "Allow64BitCounters" not in props:
                props["Allow64BitCounters"] = True
            if not params["credential_names"]:
                module.fail_json(msg="One or more credential names are required")

        elif props["ObjectSubType"] == "AGENT":
            if "agent_mode" not in params:
                module.fail_json(msg="Agent mode is required for agent polling method")
            else:
                if params["agent_mode"] == "passive":
                    if (
                        "agent_port" not in params
                        or "agent_shared_secret" not in params
                    ):
                        module.fail_json(
                            msg="Agent port and shared secret are required for agent in passive mode"
                        )
                else:
                    module.fail_json(
                        msg="Only passive agents (server-initiated communication) are currently supported"
                    )

        return props

    def discover_node(self, module, props):
        # Retrieve IDs of all credentials to be used
        discovery_credentials = []
        for i in range(len(module.params["credential_names"])):
            credential_name = module.params["credential_names"][i]
            credential = self.solarwinds.credential(module, credential_name)
            if not credential:
                module.fail_json(
                    msg="Failed to retrieve credential '{0}'".format(credential_name)
                )
            discovery_credentials.append(
                {
                    "CredentialID": self.solarwinds.credential(
                        module, module.params["credential_names"][i]
                    )["ID"],
                    "Order": i + 1,
                }
            )

        # Start to prepare our discovery profile
        core_plugin_context = {
            "BulkList": [{"Address": module.params["ip_address"]}],
            "Credentials": discovery_credentials,
            "WmiRetriesCount": ORION_DISCOVERY_WMI_RETRIES_COUNT,
            "WmiRetryIntervalMiliseconds": ORION_DISCOVERY_WMI_RETRY_INTERVAL_MS,
        }

        try:
            core_plugin_config = self.client.invoke(
                "Orion.Discovery", "CreateCorePluginConfiguration", core_plugin_context
            )
        except Exception as ex:
            module.fail_json(
                msg="Failed to create core plugin configuration: {0}".format(str(ex)),
                **props
            )

        expression_filters = [
            {"Prop": "Descr", "Op": "!Any", "Val": "null"},
            {"Prop": "Descr", "Op": "!Regex", "Val": "^$"},
        ]
        if (
            "discovery_interface_filters" in module.params
            and module.params["discovery_interface_filters"]
        ):
            expression_filters += module.params["discovery_interface_filters"]

        interfaces_plugin_context = {
            "AutoImportStatus": ["Up"],
            "AutoImportVlanPortTypes": ["Trunk", "Access", "Unknown"],
            "AutoImportVirtualTypes": ["Physical", "Virtual", "Unknown"],
            "AutoImportExpressionFilter": expression_filters,
        }

        try:
            interfaces_plugin_config = self.client.invoke(
                "Orion.NPM.Interfaces",
                "CreateInterfacesPluginConfiguration",
                interfaces_plugin_context,
            )
        except Exception as ex:
            module.fail_json(
                msg="Failed to create interfaces plugin configuration for node discovery: {0}".format(
                    str(ex)
                ),
                **props
            )

        discovery_name = "orion_node.py.{0}.{1}".format(
            module.params["node_name"], datetime.now().isoformat()
        )
        discovery_desc = "Automated discovery from orion_node.py Ansible module"
        discovery_profile = {
            "Name": discovery_name,
            "Description": discovery_desc,
            "EngineID": props["DiscoveryEngineID"],
            "JobTimeoutSeconds": ORION_DISCOVERY_JOB_TIMEOUT_SECS,
            "SearchTimeoutMiliseconds": ORION_DISCOVERY_SEARCH_TIMEOUT_MS,
            "SnmpTimeoutMiliseconds": ORION_DISCOVERY_SNMP_TIMEOUT_MS,
            "RepeatIntervalMiliseconds": ORION_DISCOVERY_REPEAT_INTERVAL_MS,
            "SnmpRetries": ORION_DISCOVERY_SNMP_RETRIES,
            "SnmpPort": module.params["snmp_port"],
            "HopCount": 0,
            "PreferredSnmpVersion": "SNMP" + str(module.params["snmp_version"]),
            "DisableIcmp": False,
            "AllowDuplicateNodes": False,
            "IsAutoImport": True,
            "IsHidden": False,
            "PluginConfigurations": [
                {"PluginConfigurationItem": core_plugin_config},
                {"PluginConfigurationItem": interfaces_plugin_config},
            ],
        }

        # Initiate discovery job with above discovery profile
        try:
            discovery_res = self.client.invoke(
                "Orion.Discovery", "StartDiscovery", discovery_profile
            )
            self.changed = True
        except Exception as ex:
            module.fail_json(
                msg="Failed to start node discovery: {0}".format(str(ex)), **props
            )
        discovery_profile_id = int(discovery_res)

        # Loop until discovery job finished
        discovery_active = True
        discovery_iter = 0
        while discovery_active:
            try:
                status_res = self.client.query(
                    "SELECT Status FROM Orion.DiscoveryProfiles WHERE ProfileID = @profile_id",
                    profile_id=discovery_profile_id,
                )
            except Exception as ex:
                module.fail_json(
                    msg="Failed to query node discovery status: {0}".format(str(ex)),
                    **props
                )
            if len(status_res["results"]) > 0:
                discovery_job_status = int(
                    next(s for s in status_res["results"])["Status"]
                )
                if discovery_job_status in [
                    ORION_DISCOVERY_JOB_STATUS_FINISHED,
                    ORION_DISCOVERY_JOB_STATUS_ERROR,
                    ORION_DISCOVERY_JOB_STATUS_NOT_COMPLETED,
                    ORION_DISCOVERY_JOB_STATUS_CANCELING,
                ]:
                    discovery_active = False
            else:
                discovery_active = False
            # Only check retries and sleep if discovery job is still in progress
            if discovery_active:
                discovery_iter += 1
                if discovery_iter >= DISCOVERY_STATUS_CHECK_RETRIES:
                    module.fail_json(
                        msg="Timeout while waiting for node discovery job to terminate",
                        **props
                    )
                time.sleep(DISCOVERY_RETRY_SLEEP_SECS)

        # Retrieve Result and BatchID to find items added to new node by discovery
        discovery_log_res = None
        try:
            discovery_log_res = self.client.query(
                " ".join(
                    [
                        "SELECT Result, ResultDescription, ErrorMessage, BatchID",
                        "FROM Orion.DiscoveryLogs WHERE ProfileID = @profile_id",
                    ]
                ),
                profile_id=discovery_profile_id,
            )
        except Exception as ex:
            module.fail_json(
                msg="Failed to query discovery logs: {0}".format(str(ex)), **props
            )
        discovery_log = discovery_log_res["results"][0]

        # Any of the below values for Result indicate a failure, so we'll abort
        if int(discovery_log["Result"]) in [
            ORION_DISCOVERY_JOB_STATUS_UNKNOWN,
            ORION_DISCOVERY_JOB_STATUS_ERROR,
            ORION_DISCOVERY_JOB_STATUS_NOT_COMPLETED,
            ORION_DISCOVERY_JOB_STATUS_CANCELING,
        ]:
            module.fail_json(
                msg="Node discovery did not complete successfully: {0}".format(
                    str(discovery_log_res)
                )
            )

        # Look up NodeID of node we discovered. We have to do all of these joins
        # because mysteriously, the NodeID in the DiscoveredNodes table has no
        # bearing on the actual NodeID of the host(s) discovered.
        try:
            discovered_nodes_res = self.client.query(
                " ".join(
                    [
                        "SELECT n.NodeID, Caption, n.Uri FROM Orion.DiscoveryProfiles dp",
                        "INNER JOIN Orion.DiscoveredNodes dn",
                        "ON dn.ProfileID = dp.ProfileID AND dn.InstanceSiteID = dp.InstanceSiteID",
                        "INNER JOIN Orion.Nodes n",
                        "ON ((n.DNS = dn.DNS AND n.InstanceSiteID = dn.InstanceSiteID)",
                        "OR (n.Caption = dn.SysName AND n.InstanceSiteID = dn.InstanceSiteID))",
                        "WHERE dp.Name = @discovery_name",
                    ]
                ),
                discovery_name=discovery_name,
            )
        except Exception as ex:
            module.fail_json(
                msg="Failed to query discovered nodes: {0}".format(str(ex)), **props
            )

        try:
            discovered_node = discovered_nodes_res["results"][0]
        except Exception as ex:
            module.fail_json(
                msg="Node '{0}' not found in discovery results (got {1}): {2}".format(
                    module.params["node_name"], discovered_nodes_res["results"], str(ex)
                ),
                **props
            )

        return discovered_node

    def get_scheduled_list_resources_status(self, module, node, job_id):
        try:
            return self.client.invoke(
                "Orion.Nodes",
                "GetScheduledListResourcesStatus",
                job_id,
                node["node_id"],
            )
        except Exception as ex:
            module.fail_json(
                msg="Failed to get list resources job status: {0}".format(str(ex))
            )

    def list_resources_for_node(self, module, props, node):
        # Initiate list resources job for node
        try:
            list_resources_res = self.client.invoke(
                "Orion.Nodes", "ScheduleListResources", node["node_id"]
            )
            self.changed = True
        except Exception as ex:
            module.fail_json(
                msg="Failed to schedule list resources job: {0}".format(str(ex)),
                **props
            )

        job_creation_pending = True
        job_retries_iter = 0
        while job_creation_pending:
            job_status_res = self.get_scheduled_list_resources_status(
                module, node, list_resources_res
            )
            if job_status_res != "Unknown":
                job_creation_pending = False
            else:
                time.sleep(LIST_RESOURCES_RETRY_SLEEP_SECS)
                job_retries_iter += 1
                if job_retries_iter >= LIST_RESOURCES_STATUS_CHECK_RETRIES:
                    module.fail_json(
                        msg="Timeout waiting for creation of ListResources job"
                    )

        job_pending = True
        while job_pending:
            job_status_res = self.get_scheduled_list_resources_status(
                module, node, list_resources_res
            )
            if job_status_res == "ReadyForImport":
                job_pending = False
            else:
                job_retries_iter += 1
                if job_retries_iter >= LIST_RESOURCES_STATUS_CHECK_RETRIES:
                    module.fail_json(
                        msg="Timeout waiting for ListResources job to terminate",
                        **props
                    )
                time.sleep(LIST_RESOURCES_RETRY_SLEEP_SECS)

        return list_resources_res

    def import_resources_for_node(self, module, props, node, job_id):
        # Import resources for node
        try:
            import_resources_res = self.client.invoke(
                "Orion.Nodes", "ImportListResourcesResult", job_id, node["node_id"]
            )
            self.changed = True
        except Exception as ex:
            module.fail_json(
                msg="Failed to import resources: {0}".format(str(ex)), **props
            )

    def filter_interfaces(self, module, props, node):
        # If we have interface filters, enumerate interfaces on the node and run
        # filters over them.
        if "interface_filters" in module.params:
            # Retrieve all interfaces for node
            try:
                node_interfaces_res = self.client.query(
                    " ".join(
                        [
                            "SELECT i.InterfaceID, i.Uri, i.Name, i.InterfaceName, i.Caption, i.FullName,",
                            "i.Alias, i.Type, i.TypeName, i.InterfaceType, i.InterfaceTypeDescription",
                            "FROM Orion.Nodes n INNER JOIN Orion.NPM.Interfaces i",
                            "ON i.NodeID = n.NodeID AND i.InstanceSiteID = n.InstanceSiteID",
                            "WHERE NodeID = @node_id",
                        ]
                    ),
                    node_id=node["node_id"],
                )
            except Exception as ex:
                module.fail_json(
                    msg="Failed to query interfaces: {0}".format(str(ex)), **props
                )

            interfaces_to_remove = []
            for entry in node_interfaces_res["results"]:
                remove_interface = False
                for interface_filter in module.params["interface_filters"]:
                    if "type" in interface_filter:
                        if re.search(
                            interface_filter["type"],
                            entry["InterfaceTypeDescription"],
                            re.IGNORECASE,
                        ):
                            remove_interface = True
                            break
                    elif "name" in interface_filter:
                        if re.search(
                            interface_filter["name"],
                            entry["DisplayName"],
                            re.IGNORECASE,
                        ):
                            remove_interface = True
                            break
                if remove_interface:
                    interfaces_to_remove.append(entry)
            if len(interfaces_to_remove) > module.params["interface_filter_cutoff_max"]:
                module.fail_json(
                    msg="Too many interfaces to remove ({0}) - aborting for safety".format(
                        str(len(interfaces_to_remove))
                    ),
                    **props
                )

            for interface in interfaces_to_remove:
                try:
                    self.client.delete(interface["Uri"])
                except Exception as ex:
                    module.fail_json(
                        msg="Failed to delete interface: {0}".format(str(ex)), **props
                    )

    def filter_volumes(self, module, props, node):
        # If we have volume filters, enumerate volumes on the node and run
        # filters over them.
        if "volume_filters" in module.params:
            # Retrieve all volumes for node
            try:
                node_volumes_res = self.client.query(
                    " ".join(
                        [
                            "SELECT v.VolumeId, v.Uri, v.Status, v.Caption, v.FullName, v.DisplayName,",
                            "v.VolumeIndex, v.VolumeType, v.DeviceId, v.VolumeDescription, v.VolumeSize",
                            "FROM Orion.Nodes n INNER JOIN Orion.Volumes v",
                            "ON v.NodeID = n.NodeID AND v.InstanceSiteID = n.InstanceSiteID",
                            "WHERE NodeID = @node_id",
                        ]
                    ),
                    node_id=node["node_id"],
                )
            except Exception as ex:
                module.fail_json(
                    msg="Failed to query volumes: {0}".format(str(ex)), **props
                )

            volumes_to_remove = []
            for entry in node_volumes_res["results"]:
                remove_volume = False
                for volume_filter in module.params["volume_filters"]:
                    if "type" in volume_filter:
                        if re.search(
                            volume_filter["type"], entry["VolumeType"], re.IGNORECASE
                        ):
                            remove_volume = True
                            break
                    elif "name" in volume_filter:
                        if re.search(
                            volume_filter["name"], entry["DisplayName"], re.IGNORECASE
                        ):
                            remove_volume = True
                            break
                if remove_volume:
                    volumes_to_remove.append(entry)
            if len(volumes_to_remove) > module.params["volume_filter_cutoff_max"]:
                module.fail_json(
                    msg="Too many volumes to remove ({0}) - aborting for safety".format(
                        str(len(volumes_to_remove))
                    ),
                    **props
                )

            for volume in volumes_to_remove:
                try:
                    self.client.delete(volume["Uri"])
                except Exception as ex:
                    module.fail_json(
                        msg="Failed to delete volume: {0}".format(str(ex)), **props
                    )

    def set_caption(self, module, props, node):
        try:
            self.client.update(node["uri"], caption=module.params["caption"])
        except Exception as ex:
            module.fail_json(
                msg="Failed to update node Caption from '{0}' to '{1}': {2}".format(
                    node["caption"], props["Caption"], str(ex)
                ),
                **props
            )

    def set_dns(self, module, props, node):
        # Set DNS name of the node
        if "DNS" in props:
            dns_name_update = {"DNS": props["DNS"]}
            try:
                self.client.update(node["uri"], **dns_name_update)
            except Exception as ex:
                module.fail_json(
                    msg="Failed to set DNS name '{0}': {1}".format(
                        props["DNS"], str(ex)
                    ),
                    **node
                )

    def set_custom_properties(self, module, props, node):
        if not props["External"]:
            # Add Custom Properties
            custom_properties = (
                module.params["custom_properties"]
                if "custom_properties" in module.params
                else {}
            )

            if isinstance(custom_properties, dict):
                for k in custom_properties.keys():
                    custom_property = {k: custom_properties[k]}
                    try:
                        self.client.update(
                            node["uri"] + "/CustomProperties", **custom_property
                        )
                        changed = True
                    except Exception as ex:
                        module.fail_json(
                            msg="Failed to add custom properties: {0}".format(str(ex)),
                            **node
                        )

    def add_node_agent(self, module, node, props):
        params = module.params
        if not params["agent_mode"] == "passive":
            module.fail_json(
                msg="Agent mode '{0}' is not currently supported".format(
                    params["agent_mode"]
                ),
                **props
            )

        agent_name = props["Caption"]
        try:
            agent_hostname = props["DNS"]
        except Exception:
            agent_hostname = params["node_name"]
        agent_ip_address = params["ip_address"]
        agent_port = params["agent_port"]
        shared_secret = params["agent_shared_secret"]
        proxy_id = 0
        auto_update_enabled = params["agent_auto_update"]

        agent_args = (
            agent_name,
            agent_hostname,
            agent_ip_address,
            agent_port,
            props["EngineID"],
            shared_secret,
            proxy_id,
            auto_update_enabled,
        )
        try:
            add_agent_res = self.client.invoke(
                "Orion.AgentManagement.Agent", "AddPassiveAgent", *agent_args
            )
        except Exception as ex:
            module.fail_json(msg="Failed to add agent: {0}".format(str(ex)), **props)

        # Only create node if it doesn't already exist
        if not node:
            node_pending = True
            node_status_iter = 0
            while node_pending:
                node = self.node(module)
                if node:
                    node_pending = False
                else:
                    node_status_iter += 1
                    if node_status_iter >= AGENT_NODE_CREATION_CHECK_RETRIES:
                        module.fail_json(
                            msg="No more retries while waiting for node to be created for new agent",
                            **props
                        )
                    time.sleep(AGENT_NODE_CREATION_SLEEP_SECS)
        return node

    def add_node_icmp(self, module, props):
        try:
            self.client.create("Orion.Nodes", **props)
        except Exception as ex:
            module.fail_json(msg="Failed to add node: {0}".format(str(ex)), **props)
        node = self.node(module)
        return node

    def add_node_snmp_wmi(self, module, props):
        # We use Orion node discovery as saved credentials can not be used when directly adding a node.
        # https://thwack.solarwinds.com/product-forums/the-orion-platform/f/orion-sdk/25327/using-orion-credential-set-for-snmpv3-when-adding-node-through-sdk
        # TODO: Enable use of credentials provided at runtime.
        self.discover_node(module, props)
        # discover_node() returns node data as an object using SWIS field names,
        # so we query the node to get the node data with internal key names.
        node = self.node(module)
        # Here we can move nodes to other polling engines after discovery.
        # For use when discovery by the desired polling engine fails.
        if props["DiscoveryEngineID"] != props["EngineID"]:
            engine_update = {"EngineID": props["EngineID"]}
            try:
                self.client.update(node["uri"], **engine_update)
            except Exception as ex:
                module.fail_json(
                    msg="Failed to move node to final polling engine '{0}': {1}".format(
                        module.params["polling_engine_name"], str(ex)
                    ),
                    **node
                )
        return node

    def add_node(self, module, node):
        # TODO: add ability to update an existing node
        props = self.validate_fields(module)
        list_resources_required = False
        if module.params["polling_method"] == "agent":
            list_resources_required = True
            node = self.add_node_agent(module, node, props)
        elif module.params["polling_method"] in POLLING_METHODS_USING_DISCOVERY:
            node = self.add_node_snmp_wmi(module, props)
        elif module.params["polling_method"] in ["external", "icmp"]:
            node = self.add_node_icmp(module, props)
        else:
            module.fail_json(
                msg="Polling method '{0}' not currently supported".format(
                    module.params["polling_method"]
                )
            )

        # Populate node metadata first, as subsequent list resources and filtering steps may fail
        if node["caption"] != module.params["caption"]:
            self.set_caption(module, props, node)
        self.set_dns(module, props, node)
        if not props["External"]:
            self.set_custom_properties(module, props, node)

        # List and import resources if required
        if list_resources_required:
            job_id = self.list_resources_for_node(module, props, node)
            self.import_resources_for_node(module, props, node, job_id)
        # Resource filtering
        self.filter_interfaces(module, props, node)
        self.filter_volumes(module, props, node)

        return dict(changed=True, msg="Node has been added", node=node)

    def remove_agent(self, module, agent):
        try:
            self.client.delete(agent["uri"])
        except Exception as ex:
            module.fail_json(msg="Error removing agent: {0}".format(str(ex)), **agent)

    def remove_node(self, module, node):
        try:
            self.client.delete(node["uri"])
        except Exception as ex:
            module.fail_json(msg="Error removing node: {0}".format(str(ex)), **node)

    def remanage_node(self, module, node):
        try:
            self.client.invoke("Orion.Nodes", "Remanage", node["netobject_id"])
        except Exception as ex:
            module.fail_json(msg=to_native(ex), exception=traceback.format_exc())
        return dict(changed=True, msg="Node has been remanaged", node=node)

    def unmanage_node(self, module, node):
        now_dt = datetime.now(self.utc)
        unmanage_from = module.params["unmanage_from"]
        unmanage_until = module.params["unmanage_until"]

        if unmanage_from:
            unmanage_from_dt = datetime.fromisoformat(unmanage_from)
        else:
            unmanage_from_dt = now_dt
        if unmanage_until:
            unmanage_until_dt = datetime.fromisoformat(unmanage_until)
        else:
            tomorrow_dt = now_dt + timedelta(days=1)
            unmanage_until_dt = tomorrow_dt

        if node["unmanaged"]:
            if (
                unmanage_from_dt.isoformat() == node["unmanage_from"]
                and unmanage_until_dt.isoformat() == node["unmanage_until"]
            ):
                module.exit_json(changed=False, node=node)

        try:
            self.client.invoke(
                "Orion.Nodes",
                "Unmanage",
                node["netobject_id"],
                str(unmanage_from_dt.astimezone(self.utc)).replace("+00:00", "Z"),
                str(unmanage_until_dt.astimezone(self.utc)).replace("+00:00", "Z"),
                False,  # use Absolute Time
                node=node,
            )
        except Exception as ex:
            module.fail_json(msg="Failed to unmanage node: {0}".format(str(ex)))
        return dict(
            changed=True,
            msg="Node will be unmanaged from {0} until {1}".format(
                unmanage_from_dt.astimezone().isoformat("T", "minutes"),
                unmanage_until_dt.astimezone().isoformat("T", "minutes"),
            ),
            node=node,
        )

    def mute_node(self, module, node):
        now_dt = datetime.now(self.utc)
        unmanage_from = module.params["unmanage_from"]
        unmanage_until = module.params["unmanage_until"]

        if unmanage_from:
            unmanage_from_dt = datetime.fromisoformat(unmanage_from)
        else:
            unmanage_from_dt = now_dt
        if unmanage_until:
            unmanage_until_dt = datetime.fromisoformat(unmanage_until)
        else:
            tomorrow_dt = now_dt + timedelta(days=1)
            unmanage_until_dt = tomorrow_dt

        unmanage_from_dt = unmanage_from_dt.astimezone()
        unmanage_until_dt = unmanage_until_dt.astimezone()

        # Check if already muted
        suppressed = self.client.invoke(
            "Orion.AlertSuppression", "GetAlertSuppressionState", [node["uri"]]
        )[0]

        # If already muted, return
        if (
            suppressed["SuppressedFrom"] == unmanage_from
            and suppressed["SuppressedUntil"] == unmanage_until
        ):
            return dict(changed=False, node=node)

        # Otherwise Mute Node with given parameters
        try:
            self.client.invoke(
                "Orion.AlertSuppression",
                "SuppressAlerts",
                [node["uri"]],
                str(unmanage_from_dt.astimezone(self.utc)).replace("+00:00", "Z"),
                str(unmanage_until_dt.astimezone(self.utc)).replace("+00:00", "Z"),
            )
        except Exception as ex:
            module.fail_json(msg="Failed to mute node: {0}".format(str(ex)))
        return dict(
            changed=True,
            msg="Node will be muted from {0} until {1}".format(
                unmanage_from_dt.astimezone().isoformat("T", "minutes"),
                unmanage_until_dt.astimezone().isoformat("T", "minutes"),
            ),
            node=node,
        )

    def unmute_node(self, module, node):
        # Check if already unmuted
        suppressed = self.client.invoke(
            "Orion.AlertSuppression", "GetAlertSuppressionState", [node["uri"]]
        )[0]

        if suppressed["SuppressionMode"] == 0:
            return dict(changed=False, node=node)

        try:
            self.client.invoke("Orion.AlertSuppression", "ResumeAlerts", [node["uri"]])
        except Exception as ex:
            module.fail_json(msg="Failed to unmute node: {0}".format(str(ex)))
        return dict(changed=True, msg="Node has been unmuted", node=node)


# ==============================================================
# main


def main():

    argument_spec = dict(
        state=dict(
            type="str",
            default="remanaged",
            choices=["present", "absent", "remanaged", "unmanaged", "muted", "unmuted"],
        ),
        node_id=dict(type="str"),
        ip_address=dict(type="str"),
        node_name=dict(type="str"),
        caption=dict(type="str"),
        unmanage_from=dict(type="str", default=None),
        unmanage_until=dict(type="str", default=None),
        polling_method=dict(
            type="str", choices=["agent", "external", "icmp", "snmp", "wmi"]
        ),
        agent_mode=dict(type="str", choices=["active", "passive"]),
        agent_port=dict(type="int", default=17790),
        agent_shared_secret=dict(type="str", no_log=True),
        agent_auto_update=dict(type="bool", default=False),
        polling_engine_id=dict(type="int"),
        polling_engine_name=dict(type="str"),
        discovery_polling_engine_name=dict(type="str"),
        snmp_version=dict(type="str", default="2c", choices=["2c", "3"]),
        snmp_port=dict(type="int", default=161),
        snmp_allow_64=dict(type="bool", default=True),
        credential_names=dict(type="list", elements="str", default=[]),
        discovery_interface_filters=dict(type="list", elements="dict", default=[]),
        interface_filters=dict(type="list", elements="dict", default=[]),
        interface_filter_cutoff_max=dict(type="int", default=10),
        volume_filters=dict(type="list", elements="dict", default=[]),
        volume_filter_cutoff_max=dict(type="int", default=50),
        custom_properties=dict(type="dict", default={}),
    )

    argument_spec.update(solarwindsclient_argument_spec())

    module = AnsibleModule(
        argument_spec=argument_spec,
        mutually_exclusive=[["polling_engine_id", "polling_engine_name"]],
        required_together=[],
        required_one_of=[["node_name", "ip_address"]],
        required_if=[
            # ["state", "present", ["polling_method"]],  # TODO: reinstate this once orion_node_info
            ["state", "muted", ["unmanage_from", "unmanage_until"]],
            ["state", "remanaged", ["unmanage_until"]],
            ["state", "unmanaged", ["unmanage_from", "unmanage_until"]],
            ["state", "unmuted", ["unmanage_until"]],
            [
                "polling_method",
                "agent",
                ["agent_mode", "agent_port", "agent_auto_update"],
            ],
            [
                "polling_method",
                "snmp",
                ["credential_names", "snmp_version", "snmp_port", "snmp_allow_64"],
            ],
            ["polling_method", "wmi", ["credential_names"]],
            ["agent_mode", "passive", ["agent_port", "agent_shared_secret"]],
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

    solarwinds = SolarWindsClient(module)
    orion_node = OrionNode(solarwinds)

    node = orion_node.node(module)
    agent = orion_node.agent(module)

    try:
        caption = module.params["caption"]
        if not isinstance(caption, str) or not caption:
            raise AssertionError("Caption not provided, not string, or empty")
    except Exception:
        try:
            caption = module.params["node_name"]
            if not isinstance(caption, str) or not caption:
                raise AssertionError("Node name not provided, not string, or empty")
        except Exception:
            caption = module.params["ip_address"]

    state = module.params["state"]
    if state == "present":
        # Do nothing if the node exists and is not an agent node,
        # or is an agent node but is connected.
        if node and (
            node["object_sub_type"].upper() != "AGENT"
            or (agent and agent["connection_status"] == ORION_CONN_STATUS_CONNECTED)
        ):
            res_args = node
        else:
            # check mode: exit changed if device doesn't exist
            if module.check_mode:
                module.exit_json(changed=True, node=node)
            else:
                # If we have an agent but no node, nix the agent
                # and let it be recreated.
                if agent:
                    orion_node.remove_agent(module, agent)
                res_args = orion_node.add_node(module, node)
    elif state == "absent":
        if node or agent:
            # check mode: exit changed if either node or agent exists
            if module.check_mode:
                module.exit_json(changed=True)
            else:
                if agent:
                    orion_node.remove_agent(module, agent)
                if node:
                    orion_node.remove_node(module, node)
                res_args = dict(changed=True, msg="Node has been removed", node=node)
        else:
            res_args = dict(
                changed=False,
                msg="Node '{0}' does not exist".format(caption),
                node=node,
            )
    else:
        if not node:
            res_args = dict(
                changed=False,
                msg="Node '{0}' does not exist".format(caption),
                node=node,
            )
        else:
            if module.check_mode:
                res_args = dict(changed=True, node=node)
            else:
                if state == "remanaged":
                    res_args = orion_node.remanage_node(module, node)
                elif state == "unmanaged":
                    res_args = orion_node.unmanage_node(module, node)
                elif state == "muted":
                    res_args = orion_node.mute_node(module, node)
                elif state == "unmuted":
                    res_args = orion_node.unmute_node(module, node)

    module.exit_json(**res_args)


if __name__ == "__main__":
    main()
