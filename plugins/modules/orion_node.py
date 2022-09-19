#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2022, Ashley Hooper <ashleyghooper@gmail.com>
# Copyright: (c) 2019, Jarett D. Chaiken <jdc@salientcg.com>
# GNU General Public License v3.0+
# (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)

__metaclass__ = type


DOCUMENTATION = r'''
---
module: orion_node

short_description: Create/removes/edit nodes in Solarwinds Orion NPM

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

  credential_name:
    description:
      - The named, existing credential to use to manage this device
    type: str

  interface_filters:
    description:
      - List of SolarWinds Orion interface discovery filters
    type: list
    elements: dict

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
'''

EXAMPLES = r'''
- name: Remove nodes
  hosts: all
  gather_facts: no
  tasks:
    - name:  Remove a node from Orion
      orion_node:
        orion_hostname: "{{ solarwinds_host }}"
        orion_username: "{{ solarwinds_username }}"
        orion_password: "{{ solarwinds_password }}"
        node_name: servername
        state: absent
      delegate_to: localhost
      throttle: 1

- name: Mute nodes
  hosts: all
  gather_facts: no
  tasks:
    - orion_node:
        orion_hostname: "{{ solarwinds_host }}"
        orion_username: "{{ solarwinds_username }}"
        orion_password: "{{ solarwinds_password }}"
        node_name: "{{ inventory_hostname }}"
        state: muted
        unmanage_from: "2020-03-13T20:58:22.033"
        unmanage_until: "2020-03-14T20:58:22.033"
      delegate_to: localhost
      throttle: 1
'''

# TODO: Add Ansible module RETURN section

from datetime import datetime, timedelta
import re
import time
import traceback

from ansible.module_utils.basic import AnsibleModule, missing_required_lib
from ansible.module_utils._text import to_native
from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.solarwinds_client import SolarwindsClient, solarwindsclient_argument_spec
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


# These control checks for node creation after an agent is registered.
AGENT_NODE_CREATION_CHECK_RETRIES = 10
AGENT_NODE_CREATION_SLEEP_SECS = 3
# These constants control how many times and at what interval this module
# will check the status of the Orion discovery job to see if it has completed.
# Total time will be retries multiplied by sleep seconds.
DISCOVERY_STATUS_CHECK_RETRIES = 60
DISCOVERY_RETRY_SLEEP_SECS = 3
# These control the discovery timeouts within Orion itself.
ORION_DISCOVERY_JOB_TIMEOUT_SECS = 300
ORION_DISCOVERY_SEARCH_TIMEOUT_MS = 20000
ORION_DISCOVERY_SNMP_TIMEOUT_MS = 30000
ORION_DISCOVERY_SNMP_RETRIES = 2
ORION_DISCOVERY_REPEAT_INTERVAL_MS = 3000
ORION_DISCOVERY_WMI_RETRIES_COUNT = 2
ORION_DISCOVERY_WMI_RETRY_INTERVAL_MS = 2000


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
        agent = {}
        results = None
        params = module.params
        if params['ip_address'] is not None:
            results = self.client.query(
                "SELECT AgentId, Name, Hostname, DNSName, IP, NodeId FROM Orion.AgentManagement.Agent WHERE IP = @ip_address",
                ip_address=params['ip_address']
            )
        elif params['node_name'] is not None:
            results = self.client.query(
                'SELECT AgentId, Name, Hostname, DNSName, IP, NodeId FROM Orion.AgentManagement.Agent WHERE Name = @agent_name OR Hostname = @agent_name OR '
                'DNSName = @agent_name',
                agent_name=params['node_name']
            )
        else:
            # No Id provided
            module.fail_json(msg="You must provide either agent_id, ip_address, or node_name")

        if results is not None:
            if 'results' in results and results['results']:
                rec = results['results'][0]
                agent = {
                    'agent_id': rec['AgentId'],
                    'name': rec['Name'],
                    'hostname': rec['Hostname'],
                    'dns_name': rec['DNSName'],
                }
        return agent

    def node(self, module):
        node = {}
        results = None
        params = module.params
        if params['node_id'] is not None:
            results = self.client.query(
                "SELECT NodeID, Caption, Unmanaged, UnManageFrom, UnManageUntil, Uri FROM Orion.Nodes WHERE NodeID = @node_id",
                node_id=params['node_id']
            )
        elif params['ip_address'] is not None:
            results = self.client.query(
                "SELECT NodeID, Caption, Unmanaged, UnManageFrom, UnManageUntil, Uri FROM Orion.Nodes WHERE IPAddress = @ip_address",
                ip_address=params['ip_address']
            )
        elif params['node_name'] is not None:
            results = self.client.query(
                "SELECT NodeID, Caption, Unmanaged, UnManageFrom, UnManageUntil, Uri FROM Orion.Nodes WHERE Caption = @node_name OR DNS = @node_name",
                node_name=params['node_name']
            )
        else:
            # No Id provided
            self.module.fail_json(msg="You must provide either node_id, ip_address, or node_name")

        if results is not None:
            if 'results' in results and results['results']:
                rec = results['results'][0]
                node = {
                    'node_id': int(rec['NodeID']),
                    'caption': rec['Caption'],
                    'netobject_id': 'N:{0}'.format(rec['NodeID']),
                    'unmanaged': rec['Unmanaged'],
                    'unmanage_from': parse(rec['UnManageFrom']).isoformat(),
                    'unmanage_until': parse(rec['UnManageUntil']).isoformat(),
                    'uri': rec['Uri']
                }
                if 'DNS' in results['results'][0]:
                    node['dns_name'] = rec['DNS']
        return node

    def validate_fields(self, module):
        # TODO: Get rid of this function and use the Ansible arguments
        # validation options and use another means to map arguments to
        # SolarWinds field names.
        params = module.params
        # Setup properties for new node
        # module.fail_json(msg='FAIL NOW', **params)
        props = {
            'IPAddress': params['ip_address'],
            'ObjectSubType': params['polling_method'].upper(),
            'External': True if params['polling_method'] == 'external' else False,
            'Caption': params['caption'] if 'caption' in params else params['node_name']
        }

        if '.' in params['node_name']:
            props['DNS'] = params['node_name']

        # Validate required fields
        if not props['IPAddress']:
            module.fail_json(msg="IP Address is required")

        if not props['External']:
            if 'node_name' not in params:
                module.fail_json(msg="Node name is required")

        if not props['ObjectSubType']:
            module.fail_json(msg="Polling Method is required [external, snmp, icmp, wmi, agent]")
        elif props['ObjectSubType'] == 'SNMP':
            props['SNMPVersion'] = params['snmp_version']
            props['SNMPPort'] = params['snmp_port']
            props['Allow64BitCounters'] = params['snmp_allow_64']
            if 'SNMPVersion' not in props:
                props['SNMPVersion'] = '2'
            if 'SNMPPort' not in props:
                props['SNMPPort'] = '161'
            if 'Allow64BitCounters' not in props:
                props['Allow64BitCounters'] = True
            if not params['credential_name']:
                module.fail_json(msg="A credential name is required")

        elif props['ObjectSubType'] == 'EXTERNAL':
            props['ObjectSubType'] = 'ICMP'

        elif props['ObjectSubType'] == 'AGENT':
            if 'agent_mode' not in params:
                module.fail_json(msg="Agent mode is required for agent polling method")
            else:
                if params['agent_mode'] == 'passive':
                    if 'agent_port' not in params or 'agent_shared_secret' not in params:
                        module.fail_json(msg="Agent port and shared secret are required for agent in passive mode")
                else:
                    module.fail_json(msg="Only passive agents (server-initiated communication) are currently supported")

        if params['polling_engine_name']:
            props['EngineID'] = self.solarwinds.polling_engine(module, params['polling_engine_name'])['EngineID']
        else:
            props['EngineID'] = 1

        if 'discovery_polling_engine_name' in params and params['discovery_polling_engine_name'] != params['polling_engine_name']:
            props['DiscoveryEngineID'] = self.solarwinds.polling_engine(module, params['discovery_polling_engine_name'])['EngineID']
        else:
            props['DiscoveryEngineID'] = props['EngineID']

        if params['state'] == 'present':
            if not props['Caption']:
                module.fail_json(msg="Node name is required")

        return props

    def discover_node(self, module, props):
        # Start to prepare our discovery profile
        core_plugin_context = {
            'BulkList': [{'Address': module.params['ip_address']}],
            'Credentials': [
                {
                    'CredentialID': self.solarwinds.credential(module),
                    'Order': 1
                }
            ],
            'WmiRetriesCount': ORION_DISCOVERY_WMI_RETRIES_COUNT,
            'WmiRetryIntervalMiliseconds': ORION_DISCOVERY_WMI_RETRY_INTERVAL_MS
        }

        try:
            core_plugin_config = self.client.invoke("Orion.Discovery", "CreateCorePluginConfiguration", core_plugin_context)
        except Exception as e:
            module.fail_json(msg="Failed to create core plugin configuration: {0}".format(str(e)), **props)

        expression_filters = [
            {"Prop": "Descr", "Op": "!Any", "Val": "null"},
            {"Prop": "Descr", "Op": "!Regex", "Val": "^$"},
        ]
        if 'interface_filters' in module.params and module.params['interface_filters']:
            expression_filters += module.params['interface_filters']

        interfaces_plugin_context = {
            "AutoImportStatus": ['Up'],
            "AutoImportVlanPortTypes": ['Trunk', 'Access', 'Unknown'],
            "AutoImportVirtualTypes": ['Physical', 'Virtual', 'Unknown'],
            "AutoImportExpressionFilter": expression_filters
        }

        try:
            interfaces_plugin_config = self.client.invoke("Orion.NPM.Interfaces", "CreateInterfacesPluginConfiguration", interfaces_plugin_context)
        except Exception as e:
            module.fail_json(msg="Failed to create interfaces plugin configuration for node discovery: {0}".format(str(e)), **props)

        discovery_name = "orion_node.py.{0}.{1}".format(module.params['node_name'], datetime.now().isoformat())
        discovery_desc = "Automated discovery from orion_node.py Ansible module"
        discovery_profile = {
            'Name': discovery_name,
            'Description': discovery_desc,
            'EngineID': props['DiscoveryEngineID'],
            'JobTimeoutSeconds': ORION_DISCOVERY_JOB_TIMEOUT_SECS,
            'SearchTimeoutMiliseconds': ORION_DISCOVERY_SEARCH_TIMEOUT_MS,
            'SnmpTimeoutMiliseconds': ORION_DISCOVERY_SNMP_TIMEOUT_MS,
            'RepeatIntervalMiliseconds': ORION_DISCOVERY_REPEAT_INTERVAL_MS,
            'SnmpRetries': ORION_DISCOVERY_SNMP_RETRIES,
            'SnmpPort': module.params['snmp_port'],
            'HopCount': 0,
            'PreferredSnmpVersion': 'SNMP' + str(module.params['snmp_version']),
            'DisableIcmp': False,
            'AllowDuplicateNodes': False,
            'IsAutoImport': True,
            'IsHidden': False,
            'PluginConfigurations': [
                {'PluginConfigurationItem': core_plugin_config},
                {'PluginConfigurationItem': interfaces_plugin_config}
            ]
        }

        # Initiate discovery job with above discovery profile
        try:
            discovery_res = self.client.invoke("Orion.Discovery", "StartDiscovery", discovery_profile)
            self.changed = True
        except Exception as e:
            module.fail_json(msg="Failed to start node discovery: {0}".format(str(e)), **props)
        discovery_profile_id = int(discovery_res)

        # Loop until discovery job finished
        # Discovery job statuses are:
        # 0 {"Unknown"} 1 {"InProgress"} 2 {"Finished"} 3 {"Error"} 4 {"NotScheduled"} 5 {"Scheduled"} 6 {"NotCompleted"} 7 {"Canceling"} 8 {"ReadyForImport"}
        # https://github.com/solarwinds/OrionSDK/blob/master/Samples/PowerShell/DiscoverSnmpV3Node.ps1
        discovery_active = True
        discovery_iter = 0
        while discovery_active:
            try:
                status_res = self.client.query("SELECT Status FROM Orion.DiscoveryProfiles WHERE ProfileID = @profile_id", profile_id=discovery_profile_id)
            except Exception as e:
                module.fail_json(msg="Failed to query node discovery status: {0}".format(str(e)), **props)
            if len(status_res['results']) > 0:
                if next(s for s in status_res['results'])['Status'] == 2:
                    discovery_active = False
            else:
                discovery_active = False
            discovery_iter += 1
            if discovery_iter >= DISCOVERY_STATUS_CHECK_RETRIES:
                module.fail_json(msg="Timeout while waiting for node discovery job to terminate", **props)
            time.sleep(DISCOVERY_RETRY_SLEEP_SECS)

        # Retrieve Result and BatchID to find items added to new node by discovery
        discovery_log_res = None
        try:
            discovery_log_res = self.client.query(
                "SELECT Result, ResultDescription, ErrorMessage, BatchID "
                "FROM Orion.DiscoveryLogs WHERE ProfileID = @profile_id",
                profile_id=discovery_profile_id
            )
        except Exception as e:
            module.fail_json(msg="Failed to query discovery logs: {0}".format(str(e)), **props)
        discovery_log = discovery_log_res['results'][0]

        # Any of the below values for Result indicate a failure, so we'll abort
        if int(discovery_log['Result']) in [0, 3, 6, 7]:
            module.fail_json(msg="Node discovery did not complete successfully: {0}".format(str(discovery_log_res)))

        # Look up NodeID of node we discovered. We have to do all of these joins
        # because mysteriously, the NodeID in the DiscoveredNodes table has no
        # bearing on the actual NodeID of the host(s) discovered.
        try:
            discovered_nodes_res = self.client.query(
                "SELECT n.NodeID, Caption, n.Uri FROM Orion.DiscoveryProfiles dp "
                "INNER JOIN Orion.DiscoveredNodes dn ON dn.ProfileID = dp.ProfileID "
                "INNER JOIN Orion.Nodes n ON n.DNS = dn.DNS OR n.Caption = dn.SysName "
                "WHERE dp.Name = @discovery_name",
                discovery_name=discovery_name
            )
        except Exception as e:
            module.fail_json(msg="Failed to query discovered nodes: {0}".format(str(e)), **props)

        try:
            discovered_node = discovered_nodes_res['results'][0]
        except Exception as e:
            module.fail_json(
                msg="Node '{0}' not found in discovery results (got {1}): {2}".format(
                    module.params['node_name'],
                    discovered_nodes_res['results'], str(e)
                ),
                **props
            )

        return discovered_node

    def get_scheduled_list_resources_status(self, module, node, job_id):
        try:
            return self.client.invoke("Orion.Nodes", "GetScheduledListResourcesStatus", job_id, node['node_id'])
        except Exception as e:
            module.fail_json(msg="Failed to get list resources job status: {0}".format(str(e)))

    def list_resources_for_node(self, module, props, node):
        # Initiate list resources job for node
        try:
            list_resources_res = self.client.invoke("Orion.Nodes", "ScheduleListResources", node['node_id'])
            self.changed = True
        except Exception as e:
            module.fail_json(msg="Failed to schedule list resources job: {0}".format(str(e)), **props)

        job_creation_pending = True
        job_creation_status_iter = 0
        while job_creation_pending:
            job_status_res = self.get_scheduled_list_resources_status(module, node, list_resources_res)
            if job_status_res != 'Unknown':
                job_creation_pending = False
            else:
                time.sleep(DISCOVERY_RETRY_SLEEP_SECS)
                job_creation_status_iter += 1
                if job_creation_status_iter >= DISCOVERY_STATUS_CHECK_RETRIES:
                    module.fail_json(msg="Timeout waiting for creation of list resources job")

        job_pending = True
        job_status_iter = 0
        while job_pending:
            job_status_res = self.get_scheduled_list_resources_status(module, node, list_resources_res)
            if job_status_res == 'ReadyForImport':
                job_pending = False
            else:
                job_status_iter += 1
                if job_status_iter >= DISCOVERY_STATUS_CHECK_RETRIES:
                    module.fail_json(msg="Timeout while waiting for list resources job to terminate", **props)
                time.sleep(DISCOVERY_RETRY_SLEEP_SECS)

        return list_resources_res

    def import_resources_for_node(self, module, props, node, job_id):
        # Import resources for node
        try:
            import_resources_res = self.client.invoke("Orion.Nodes", "ImportListResourcesResult", job_id, node['node_id'])
            self.changed = True
        except Exception as e:
            module.fail_json(msg="Failed to import resources: {0}".format(str(e)), **props)

    def filter_volumes(self, module, props, node):
        # If we have volume filters, enumerate volumes on the node and run
        # filters over them.
        if 'volume_filters' in module.params:
            # Retrieve all volumes for node
            try:
                node_volumes_res = self.client.query(
                    "SELECT v.VolumeId, v.Uri, v.Status, v.Caption, v.FullName, v.DisplayName, v.VolumeIndex, "
                    "v.VolumeType, v.DeviceId, v.VolumeDescription, v.VolumeSize "
                    "FROM Orion.Nodes n INNER JOIN Orion.Volumes v ON v.NodeID = n.NodeID WHERE NodeID = @node_id",
                    node_id=node['node_id']
                )
            except Exception as e:
                module.fail_json(msg="Failed to query volumes: {1}".format(str(e)), **props)

            volumes_to_remove = []
            for entry in node_volumes_res['results']:
                remove_volume = False
                for volume_filter in module.params['volume_filters']:
                    if 'type' in volume_filter:
                        if re.search(volume_filter['type'], entry['VolumeType']):
                            remove_volume = True
                            break
                    elif 'name' in volume_filter:
                        if re.search(volume_filter['name'], entry['DisplayName']):
                            remove_volume = True
                            break
                if remove_volume:
                    volumes_to_remove.append(entry)
            if len(volumes_to_remove) > module.params['volume_filter_cutoff_max']:
                module.fail_json(msg="Too many volumes to remove ({0}) - aborting for safety".format(str(len(volumes_to_remove))), **props)

            for volume in volumes_to_remove:
                try:
                    self.client.delete(volume['Uri'])
                except Exception as e:
                    module.fail_json(msg="Failed to delete volume: {0}".format(str(e)), **props)

    def set_caption(self, module, props, node):
        try:
            self.client.update(node['uri'], caption=module.params['caption'])
        except Exception as e:
            module.fail_json(msg="Failed to update node Caption from '{0}' to '{1}': {2}".format(node['caption'], props['Caption'], str(e)), **props)

    def set_dns(self, module, props, node):
        # Set DNS name of the node
        if 'DNS' in props:
            dns_name_update = {
                "DNS": props['DNS']
            }
            try:
                self.client.update(node['uri'], **dns_name_update)
            except Exception as e:
                module.fail_json(msg="Failed to set DNS name '{0}': {1}".format(props['DNS'], str(e)), **node)

    def set_custom_properties(self, module, props, node):
        if not props['External']:
            # Add Custom Properties
            custom_properties = module.params['custom_properties'] if 'custom_properties' in module.params else {}

            if isinstance(custom_properties, dict):
                for k in custom_properties.keys():
                    custom_property = {k: custom_properties[k]}
                    try:
                        self.client.update(node['uri'] + "/CustomProperties", **custom_property)
                        changed = True
                    except Exception as e:
                        module.fail_json(msg="Failed to add custom properties: {0}".format(str(e)), **node)

    def add_node_agent(self, module, props):
        params = module.params
        if not params['agent_mode'] == 'passive':
            module.fail_json(msg="Agent mode '{0}' is not currently supported".format(params['agent_mode']), **props)

        agent_name = props['Caption']
        try:
            agent_hostname = props['DNS']
        except Exception:
            agent_hostname = params['node_name']
        agent_ip_address = params['ip_address']
        agent_port = params['agent_port']
        shared_secret = params['agent_shared_secret']
        proxy_id = 0
        auto_update_enabled = params['agent_auto_update']

        agent_args = (
            agent_name,
            agent_hostname,
            agent_ip_address,
            agent_port,
            props['EngineID'],
            shared_secret,
            proxy_id,
            auto_update_enabled
        )
        try:
            add_agent_res = self.client.invoke("Orion.AgentManagement.Agent", "AddPassiveAgent", *agent_args)
        except Exception as e:
            module.fail_json(msg="Failed to add agent: {0}".format(str(e)), **props)

        node_pending = True
        node_status_iter = 0
        while node_pending:
            node = self.node(module)
            if node:
                node_pending = False
            else:
                node_status_iter += 1
                if node_status_iter >= AGENT_NODE_CREATION_CHECK_RETRIES:
                    module.fail_json(msg="No more retries while waiting for node to be created for new agent", **props)
                time.sleep(AGENT_NODE_CREATION_SLEEP_SECS)

        job_id = self.list_resources_for_node(module, props, node)
        self.import_resources_for_node(module, props, node, job_id)
        return node

    def add_node_snmp_wmi(self, module, props):
        self.discover_node(module, props)
        # discover_node() returns node data as an object using SWIS field names,
        # so we query the node to get the node data with internal key names.
        node = self.node(module)
        # Here we can move nodes to other polling engines after discovery.
        # For use when discovery by the desired polling engine fails.
        if props['DiscoveryEngineID'] != props['EngineID']:
            engine_update = {
                "EngineID": props['EngineID']
            }
            try:
                self.client.update(node['uri'], **engine_update)
            except Exception as e:
                module.fail_json(msg="Failed to move node to final polling engine '{0}': {1}".format(module.params['polling_engine_name'], str(e)), **node)
        return node

    def add_node(self, module):
        # TODO: add ability to update an existing node

        # Validate Fields
        props = self.validate_fields(module)

        if module.params['polling_method'] == 'agent':
            node = self.add_node_agent(module, props)
        elif module.params['polling_method'] in ['snmp', 'wmi']:
            node = self.add_node_snmp_wmi(module, props)
        # TODO: external/icmp nodes
        #  elif module.params['polling_method'] == 'external':
            #  self.add_node_external(module, props)
        #  elif module.params['polling_method'] == 'icmp':
            #  self.add_node_icmp(module, props)
        else:
            module.fail_json(msg="Polling method '{0}' not currently supported".format(module.params['polling_method']))

        # Filter interfaces and volumes, set DNS, caption, custom properties, etc
        self.filter_volumes(module, props, node)
        if node['caption'] != module.params['caption']:
            self.set_caption(module, props, node)
        self.set_dns(module, props, node)
        if not props['External']:
            self.set_custom_properties(module, props, node)
        module.exit_json(**node)

        return dict(changed=True, msg="Node has been added")

    def remove_node(self, module, node):
        # TODO: Check for agent installation and remove, if requested
        try:
            self.client.delete(node['uri'])
        except Exception as e:
            module.fail_json(msg="Error removing node: {0}".format(str(e)), **node)

    def remanage_node(self, module, node):
        try:
            self.client.invoke("Orion.Nodes", "Remanage", node['netobject_id'])
        except Exception as e:
            module.fail_json(msg=to_native(e), exception=traceback.format_exc())
        return dict(changed=True, msg="Node has been remanaged")

    def unmanage_node(self, module, node):
        now_dt = datetime.now(self.utc)
        unmanage_from = module.params['unmanage_from']
        unmanage_until = module.params['unmanage_until']

        if unmanage_from:
            unmanage_from_dt = datetime.fromisoformat(unmanage_from)
        else:
            unmanage_from_dt = now_dt
        if unmanage_until:
            unmanage_until_dt = datetime.fromisoformat(unmanage_until)
        else:
            tomorrow_dt = now_dt + timedelta(days=1)
            unmanage_until_dt = tomorrow_dt

        if node['unmanaged']:
            if unmanage_from_dt.isoformat() == node['unmanage_from'] and unmanage_until_dt.isoformat() == node['unmanage_until']:
                module.exit_json(changed=False)

        try:
            self.client.invoke(
                "Orion.Nodes",
                "Unmanage",
                node['netobject_id'],
                str(unmanage_from_dt.astimezone(self.utc)).replace("+00:00", "Z"),
                str(unmanage_until_dt.astimezone(self.utc)).replace("+00:00", "Z"),
                False  # use Absolute Time
            )
        except Exception as e:
            module.fail_json(msg="Failed to unmanage node: {0}".format(str(e)))
        return dict(
            changed=True,
            msg="Node will be unmanaged from {0} until {1}".format(
                unmanage_from_dt.astimezone().isoformat("T", "minutes"),
                unmanage_until_dt.astimezone().isoformat("T", "minutes")
            )
        )

    def mute_node(self, module, node):
        now_dt = datetime.now(self.utc)
        unmanage_from = module.params['unmanage_from']
        unmanage_until = module.params['unmanage_until']

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
        suppressed = self.client.invoke("Orion.AlertSuppression", "GetAlertSuppressionState", [node['uri']])[0]

        # If already muted, return
        if suppressed['SuppressedFrom'] == unmanage_from and suppressed['SuppressedUntil'] == unmanage_until:
            return dict(changed=False)

        # Otherwise Mute Node with given parameters
        try:
            self.client.invoke(
                "Orion.AlertSuppression",
                "SuppressAlerts",
                [node['uri']],
                str(unmanage_from_dt.astimezone(self.utc)).replace("+00:00", "Z"),
                str(unmanage_until_dt.astimezone(self.utc)).replace("+00:00", "Z")
            )
        except Exception as e:
            module.fail_json(msg="Failed to mute node: {0}".format(str(e)))
        return dict(
            changed=True,
            msg="Node will be muted from {0} until {1}".format(
                unmanage_from_dt.astimezone().isoformat("T", "minutes"),
                unmanage_until_dt.astimezone().isoformat("T", "minutes")
            )
        )

    def unmute_node(self, module, node):
        # Check if already unmuted
        suppressed = self.client.invoke("Orion.AlertSuppression", "GetAlertSuppressionState", [node['uri']])[0]

        if suppressed['SuppressionMode'] == 0:
            return dict(changed=False)

        try:
            self.client.invoke("Orion.AlertSuppression", "ResumeAlerts", [node['uri']])
        except Exception as e:
            module.fail_json(msg="Failed to unmute node: {0}".format(str(e)))
        return dict(
            changed=True,
            msg="Node has been unmuted"
        )


# ==============================================================
# main

def main():

    argument_spec = dict(
        state=dict(type='str', default='remanaged', choices=['present', 'absent', 'remanaged', 'unmanaged', 'muted', 'unmuted']),
        node_id=dict(type='str'),
        ip_address=dict(type='str'),
        node_name=dict(type='str'),
        caption=dict(type='str'),
        unmanage_from=dict(type='str', default=None),
        unmanage_until=dict(type='str', default=None),
        polling_method=dict(type='str', choices=['agent', 'external', 'icmp', 'snmp', 'wmi']),
        agent_mode=dict(type='str', choices=['active', 'passive']),
        agent_port=dict(type='int', default=17790),
        agent_shared_secret=dict(type='str', no_log=True),
        agent_auto_update=dict(type='bool', default=False),
        polling_engine_id=dict(type='int'),
        polling_engine_name=dict(type='str'),
        discovery_polling_engine_name=dict(type='str'),
        snmp_version=dict(type='str', default='2c', choices=['2c', '3']),
        snmp_port=dict(type='int', default=161),
        snmp_allow_64=dict(type='bool', default=True),
        credential_name=dict(type='str'),
        interface_filters=dict(type='list', elements='dict', default=[]),
        volume_filters=dict(type='list', elements='dict', default=[]),
        volume_filter_cutoff_max=dict(type='int', default=50),
        custom_properties=dict(type='dict', default={})
    )

    argument_spec.update(solarwindsclient_argument_spec())

    module = AnsibleModule(
        argument_spec=argument_spec,
        mutually_exclusive=[['polling_engine_id', 'polling_engine_name']],
        required_together=[],
        required_if=[
            ['polling_method', 'agent', ['agent_mode', 'agent_port', 'agent_auto_update']],
            ['polling_method', 'snmp', ['credential_name', 'snmp_version', 'snmp_port', 'snmp_allow_64']],
            ['polling_method', 'wmi', ['credential_name']],
            ['agent_mode', 'passive', ['agent_port', 'agent_shared_secret']],
            ['state', 'muted', ['unmanage_from', 'unmanage_until']],
            ['state', 'remanaged', ['unmanage_until']],
            ['state', 'unmanaged', ['unmanage_from', 'unmanage_until']],
            ['state', 'unmuted', ['unmanage_until']],
        ],
        supports_check_mode=True
    )

    if not HAS_DATEUTIL:
        module.fail_json(msg=missing_required_lib('dateutil'), exception=DATEUTIL_IMPORT_ERROR)
    if not HAS_REQUESTS:
        module.fail_json(msg=missing_required_lib('requests'), exception=REQUESTS_IMPORT_ERROR)

    state = module.params['state']
    try:
        caption = module.params['caption']
    except Exception:
        try:
            caption = module.params['node_name']
        except Exception:
            caption = module.params['ip_address']
    #  node_id = module.params['node_id']
    #  node_name = module.params['node_name']
    #  ip_address = module.params['ip_address']
    #  polling_method = module.params['polling_method']
    #  agent_mode = module.params['agent_mode']
    #  agent_port = module.params['agent_port']
    #  agent_shared_secret = module.params['agent_shared_secret']
    #  agent_auto_update = module.params['agent_auto_update']
    #  polling_engine_id = module.params['polling_engine_id']
    #  polling_engine_name = module.params['polling_engine_name']
    #  discovery_polling_engine_name = module.params['discovery_polling_engine_name']
    #  snmp_version = module.params['snmp_version']
    #  snmp_port = module.params['snmp_port']
    #  snmp_allow_64 = module.params['snmp_allow_64']
    #  credential_name = module.params['credential_name']
    #  interface_filters = module.params['interface_filters']
    #  volume_filters = module.params['volume_filters']
    #  volume_filter_cutoff_max = module.params['volume_filter_cutoff_max']
    #  custom_properties = module.params['custom_properties']

    solarwinds = SolarwindsClient(module)
    orion_node = OrionNode(solarwinds)

    node = orion_node.node(module)

    if state == 'present':
        if node:
            res_args = node
        else:
            # check mode: exit changed if device doesn't exist
            if module.check_mode:
                module.exit_json(changed=True)
            else:
                res_args = orion_node.add_node(module)
    elif state == 'absent':
        if node:
            # check mode: exit changed if device exists
            if module.check_mode:
                module.exit_json(changed=True)
            else:
                orion_node.remove_node(module, node)
                res_args = dict(changed=True, msg="Node has been removed")
        else:
            res_args = node
    else:
        if not node:
            res_args = dict(
                changed=False,
                msg="Node '{0}' not found in solarwinds".format(caption)
            )
        else:
            if module.check_mode:
                res_args = dict(changed=True)
            else:
                if state == 'remanaged':
                    res_args = orion_node.remanage_node(module, node)
                elif state == 'unmanaged':
                    res_args = orion_node.unmanage_node(module, node)
                elif state == 'muted':
                    res_args = orion_node.mute_node(module, node)
                elif state == 'unmuted':
                    res_args = orion_node.unmute_node(module, node)

    module.exit_json(**res_args)


if __name__ == "__main__":
    main()