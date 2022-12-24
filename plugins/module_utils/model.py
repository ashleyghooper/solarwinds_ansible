# -*- coding: utf-8 -*-
#
# Copyright: (c) 2022, Ashley Hooper <ashleyghooper@gmail.com>
#
# GNU General Public License v3.0+
# (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type


class Model(object):
    """
    Representation of relevant tables from the SolarWinds Information Service
    schema v3.0 (https://solarwinds.github.io/OrionSDK/schema/index.html).
    """

    tables = [
        "Nodes",
        "Agents",
        "NodesCustomProperties",
        # "Volumes",
    ]

    def __init__(self, solarwinds, tables_columns=None):
        self.table_instances = {}
        if tables_columns is None:
            self.query_tables = self.tables
        else:
            self.query_tables = [t for t in tables_columns.keys()]
        for table in self.query_tables:
            if table not in self.tables:
                raise ValueError(
                    "Table '{0!r}' does not exist or is not a usable table".format(
                        table
                    )
                )
            self.table_instances[table] = eval(table)(solarwinds, tables_columns[table])
            for column in tables_columns[table]:
                if column not in self.table_instances[table].all_columns:
                    raise ValueError(
                        "Column '{0}' of table '{1}' does not exist or is not usable".format(
                            column, table
                        )
                    )

    def query_columns(self):
        columns = []
        for table in self.table_instances:
            columns += [
                ".".join([self.table_instances[table].alias, t])
                for t in self.table_instances[table].query_columns
            ]
        return columns

        # columns = []
        # table_map = dict(
        #     Nodes=dict(alias="n", boolean_columns=["Allow64BitCounters", "Unmanaged"]),
        #     Agents=dict(
        #         alias="a",
        #         join="Orion.AgentManagement.Agent a ON a.NodeID = n.NodeID",
        #         boolean_columns=["AutoUpdateEnabled"],
        #     ),
        #     CustomProperties=dict(
        #         alias="cp",
        #         join="Orion.NodesCustomProperties cp ON cp.NodeID = n.NodeID",
        #     ),
        # )
        # for table in table_map:
        #     table_def = table_map[table]
        #     for column in module.params["columns"][table]:
        #         columns.append(".".join([table_def["alias"], column]))


class Nodes(object):

    alias = "n"
    schema = "Orion"

    all_columns = [
        "NodeID",
        "ObjectSubType",
        "IPAddress",
        "IPAddressType",
        "DynamicIP",
        "Caption",
        "NodeDescription",
        "Description",
        "DNS",
        "SysName",
        "Vendor",
        "SysObjectID",
        "Location",
        "Contact",
        "VendorIcon",
        "Icon",
        "Status",
        "StatusLED",
        "StatusDescription",
        "CustomStatus",
        "IOSImage",
        "IOSVersion",
        "GroupStatus",
        "StatusIcon",
        "LastBoot",
        "SystemUpTime",
        "ResponseTime",
        "PercentLoss",
        "AvgResponseTime",
        "MinResponseTime",
        "MaxResponseTime",
        "CPUCount",
        "CPULoad",
        "MemoryUsed",
        "LoadAverage1",
        "LoadAverage5",
        "LoadAverage15",
        "MemoryAvailable",
        "PercentMemoryUsed",
        "PercentMemoryAvailable",
        "LastSync",
        "LastSystemUpTimePollUtc",
        "MachineType",
        "IsServer",
        "Severity",
        "UiSeverity",
        "ChildStatus",
        "Allow64BitCounters",
        "AgentPort",
        "TotalMemory",
        "CMTS",
        "CustomPollerLastStatisticsPoll",
        "CustomPollerLastStatisticsPollSuccess",
        "SNMPVersion",
        "PollInterval",
        "EngineID",
        "RediscoveryInterval",
        "NextPoll",
        "NextRediscovery",
        "StatCollection",
        "External",
        "Community",
        "RWCommunity",
        "IP",
        "IP_Address",
        "IPAddressGUID",
        "NodeName",
        "BlockUntil",
        "BufferNoMemThisHour",
        "BufferNoMemToday",
        "BufferSmMissThisHour",
        "BufferSmMissToday",
        "BufferMdMissThisHour",
        "BufferMdMissToday",
        "BufferBgMissThisHour",
        "BufferBgMissToday",
        "BufferLgMissThisHour",
        "BufferLgMissToday",
        "BufferHgMissThisHour",
        "BufferHgMissToday",
        "OrionIdPrefix",
        "OrionIdColumn",
        "SkippedPollingCycles",
        "MinutesSinceLastSync",
        "EntityType",
        "DetailsUrl",
        "DisplayName",
        "Category",
        "IsOrionServer",
        "Status",
        "StatusDescription",
        "StatusLED",
        "UnManaged",
        "UnManageFrom",
        "UnManageUntil",
        "DetailsUrl",
        "Image",
        "AncestorDisplayNames",
        "AncestorDetailsUrls",
        "StatusIconHint",
        "DisplayName",
        "Description",
        "InstanceType",
        "Uri",
        "InstanceSiteId",
    ]

    boolean_columns = [
        "Allow64BitCounters",
        "CustomStatus",
        "DynamicIP",
        "External",
        "IsOrionServer",
        "IsServer",
        "Unmanaged",
    ]

    def __init__(self, solarwinds, query_columns=None):
        if query_columns is None:
            self.query_columns = self.all_columns
        else:
            self.query_columns = query_columns


class Agents(object):

    alias = "a"
    schema = "Orion"

    all_columns = [
        "AgentId",
        "AgentGuid",
        "NodeId",
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
        "OSType",
        "OSDistro",
        "ResponseTime",
        "Type",
        "RuntimeOSDistro",
        "RuntimeOSVersion",
        "RuntimeOSLabel",
        "OSLabel",
        "DisplayName",
        "Description",
        "InstanceType",
        "Uri",
        "InstanceSiteId",
    ]

    boolean_columns = [
        "AutoUpdateEnabled",
        "Is64Windows",
        "IsActiveAgent",
    ]

    joins = dict(Nodes=["NodeID", "NodeID"])

    def __init__(self, solarwinds, query_columns=None):
        if query_columns is None:
            self.query_columns = self.all_columns
        else:
            self.query_columns = query_columns


class NodesCustomProperties(object):

    alias = "cp"
    schema = "Orion"

    boolean_columns = [
        "AutoUpdateEnabled",
        "Is64Windows",
        "IsActiveAgent",
    ]

    joins = dict(Nodes=["NodeID", "NodeID"])

    def __init__(self, solarwinds, query_columns=None):
        if query_columns is None:
            all_columns_query = "SELECT Field FROM Orion.CustomProperty WHERE TargetEntity = 'Orion.NodesCustomProperties'"
            query_res = self.solarwinds.client.query(all_columns_query)
            if "results" in query_res and query_res["results"]:
                self.all_columns = query_res["results"]
                self.query_columns = self.all_columns
            else:
                raise Exception("Failed to look up Custom Properties")
        else:
            self.query_columns = query_columns
