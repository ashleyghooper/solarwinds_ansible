# -*- coding: utf-8 -*-
#
# Copyright: (c) 2022, Ashley Hooper <ashleyghooper@gmail.com>
#
# GNU General Public License v3.0+
# (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type


import traceback

from ansible.module_utils.basic import missing_required_lib

ORIONSDK_IMPORT_ERROR = None
try:
    from orionsdk import SwisClient
except ImportError:
    HAS_ORIONSDK = False
    ORIONSDK_IMPORT_ERROR = traceback.format_exc()
else:
    HAS_ORIONSDK = True


def solarwindsclient_argument_spec():
    options = dict(
        hostname=dict(type="str", required=True),
        username=dict(type="str", required=True, no_log=True),
        password=dict(type="str", required=True, no_log=True),
    )

    return dict(
        solarwinds_connection=dict(type="dict", apply_defaults=True, options=options)
    )


def check_client(module):
    if not HAS_ORIONSDK:
        module.fail_json(
            msg=missing_required_lib("orionsdk"), exception=ORIONSDK_IMPORT_ERROR
        )


def validate_connection_params(module):
    params = module.params["solarwinds_connection"]
    error_str = "missing required argument: solarwinds_connection[{}]"
    hostname = params["hostname"]
    username = params["username"]
    password = params["password"]

    if hostname and username and password:
        return params
    for arg in ["hostname", "username", "password"]:
        if params[arg] in (None, ""):
            module.fail_json(msg=error_str.format(arg))


class SolarwindsClient(object):
    """
    Class encapsulating SolarWinds Information Service API client.
    """

    def __init__(self, module):
        # handle import errors
        check_client(module)

        params = validate_connection_params(module)

        hostname = params["hostname"]
        username = params["username"]
        password = params["password"]

        self._module = module
        self._hostname = hostname
        self._auth = dict(username=username, password=password)
        try:
            self._client = SwisClient(self._hostname, **self._auth)
        except Exception as ex:
            self.module.fail_json(
                msg="failed to open connection (%s): %s" % (hostname, str(ex))
            )

        try:
            self._client.query("SELECT uri FROM Orion.Environment")
        except Exception as ex:
            module.fail_json(
                msg="Failed to query Orion. "
                "Check Orion hostname, username, and/or password: {0}".format(str(ex))
            )

    @property
    def module(self):
        """Ansible module module
        Returns:
            the ansible module
        """
        return self._module

    @property
    def hostname(self):
        """SolarWinds Information Service server hostname
        Returns:
            the SolarWinds Information Service server hostname.
        """
        return self._hostname

    @property
    def client(self):
        """SolarWinds Information Service client.
        Returns:
            the SolarWinds Information Service client.
        """
        return self._client

    def entity(self, entity_uri):
        """Search for entity by uri.
        Returns:
            the entity, or None if entity was not found.
        """
        entity = None
        if entity_uri is not None:
            entity = self._client.read(entity_uri)
        return entity

    def credential(self, module, credential_name):
        """Search for credential by name.
        Returns:
            the credential if found, or None
        """
        try:
            credentials_res = self._client.query(
                "SELECT ID FROM Orion.Credential WHERE Name = @credential_name",
                credential_name=credential_name,
            )
            return next((c for c in credentials_res["results"]), None)
        except Exception as ex:
            module.fail_json(
                msg="Failed to retrieve credential '{0}': {1}".format(
                    credential_name, str(ex)
                )
            )

    def polling_engine(self, module, polling_engine_name):
        """Search for polling engine by name.
        Returns:
            the polling engine if found, or None
        """
        try:
            engines_res = self._client.query(
                "SELECT EngineID, ServerName, PollingCompletion FROM Orion.Engines WHERE ServerName = @engine_name",
                engine_name=polling_engine_name,
            )
            return next((e for e in engines_res["results"]), None)
        except Exception as ex:
            module.fail_json(
                msg="Failed to retrieve polling engine '{0}': {1}".format(
                    polling_engine_name, str(ex)
                )
            )

    def relationships(self, module, source_table, target_tables):
        """Retrieve table relationships for the given types from the Metadata.Relationship table.
        Returns:
            a list of relationships if found, or None
        """
        try:
            table_filters = " ".join(
                [
                    ("OR " if i > 0 else "")
                    + "('{0}' IN (SourceType, TargetType) AND '{1}' IN (SourceType, TargetType))".format(
                        source_table, t
                    )
                    for i, t in enumerate(
                        [t for t in target_tables if t != source_table]
                    )
                ]
            )
            # module.fail_json(
            #     msg="{0}\n{1}\n{2}".format(
            #         source_type, str(target_types), entity_filters
            #     )
            # )
            relationships_res = self._client.query(
                " ".join(
                    [
                        "SELECT SourceType, TargetType, SourcePrimaryKeyNames,",
                        "SourceForeignKeyNames, SourceCardinalityMin,",
                        "SourceCardinalityMax, TargetPrimaryKeyNames,",
                        "TargetForeignKeyNames, TargetCardinalityMin,",
                        "TargetCardinalityMax",
                        "FROM Metadata.Relationship",
                        "WHERE InstanceType = 'Metadata.Relationship'",
                        "AND (",
                        table_filters,
                        ")",
                    ]
                )
            )
            table_relations = [
                t["TargetType"]
                for t in relationships_res["results"]
                if t["SourceType"] == source_table
            ]
            table_relations_set = set(table_relations)
            unmatched_tables = [
                t for t in target_tables if t not in table_relations_set
            ]
            if unmatched_tables:
                raise Exception(
                    "No relationship found between entities '{0}' and {1}".format(
                        source_table, str(unmatched_tables)
                    )
                )
            return {
                r["TargetType"]: [
                    t
                    for t in relationships_res["results"]
                    if t["SourceType"] == source_table
                ]
                for r in relationships_res["results"]
            }
        except Exception as ex:
            module.fail_json(
                msg="Failed to retrieve table relationships between '{0}' and {1}: {2}".format(
                    source_table, str(target_tables), str(ex)
                )
            )

    def properties(self, module, tables):
        """Retrieve the list of properties for a list of tables from the Metadata.Property table.
        Returns:
            a dictionary of tables each containing a dictionary of columns, or None
        """
        try:
            table_filters = "".join(
                [
                    (", " if i > 0 else "") + "'{0}'".format(t)
                    for i, t in enumerate(tables)
                ]
            )
            properties_res = self._client.query(
                " ".join(
                    [
                        "SELECT EntityName, Name, Type, IsMetric, Units, MaxValue,",
                        "MinValue, Values, IsNavigable, IsKey, IsNullable,",
                        "IsInherited, IsInjected, IsSortable, GroupBy,",
                        "FilterBy, CanCreate, CanRead, CanUpdate, Events,",
                        "DisplayName, Description, InstanceType, Uri,",
                        "InstanceSiteId",
                        "FROM Metadata.Property",
                        "WHERE EntityName IN (",
                        table_filters,
                        ")",
                        "AND IsNavigable = false",
                    ]
                )
            )
            return {
                r["EntityName"]: {
                    p["Name"]: p
                    for p in properties_res["results"]
                    if p["EntityName"] == r["EntityName"]
                }
                for r in properties_res["results"]
            }
        except Exception as ex:
            module.fail_json(
                msg="Failed to retrieve entity properties for {0}: {1}".format(
                    str(tables), str(ex)
                )
            )
