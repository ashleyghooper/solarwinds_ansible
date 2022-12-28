# -*- coding: utf-8 -*-
#
# Copyright: (c) 2022, Ashley Hooper <ashleyghooper@gmail.com>
#
# GNU General Public License v3.0+
# (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

import re
import traceback
from datetime import datetime

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


def case_insensitive_key(d, k):
    k = k.lower()
    return [d[key] for key in d if key.lower() == k]


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


class SolarwindsQuery(object):
    """
    Class for facilitating dynamic SolarWinds Information Service queries.
    """

    @property
    def input_filters(self):
        return self._input_filters

    @property
    def input_columns(self):
        return self._input_columns

    @input_filters.setter
    def input_filters(self, value):
        self._input_filters = value

    @input_columns.setter
    def input_columns(self, value):
        self._input_columns = value

    def __init__(self, module, solarwinds_client, base_table):
        self._module = module
        self._client = solarwinds_client
        self.base_table = base_table

    def entity_properties(self, input_tables_set):
        try:
            table_filters = "".join(
                [
                    (", " if i > 0 else "") + "'{0}'".format(t)
                    for i, t in enumerate(input_tables_set)
                ]
            )
            query_res = self._client.query(
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

            properties = {
                r["EntityName"]: {
                    p["Name"]: p
                    for p in query_res["results"]
                    if p["EntityName"] == r["EntityName"]
                }
                for r in query_res["results"]
            }
            return properties
        except Exception as ex:
            self._module.fail_json(
                msg="Failed to retrieve entity properties for {0}: {1}".format(
                    str(input_tables_set), str(ex)
                )
            )

    def valid_filter(self, data_type, filter_content):
        if isinstance(filter_content, dict):
            for key in filter_content.keys():
                if key in ["max", "min", "not"]:
                    valid = self.valid_filter(data_type, filter_content[key])
                if not valid:
                    return False
        elif isinstance(filter_content, list):
            criteria = filter_content
        else:
            criteria = [filter_content]
        for criterion in [c for c in criteria if c is not None]:
            if data_type not in [
                "System.String",
                "System.Type",
                "System.Guid",
                "System.DateTime",
                "System.Int32",
                "System.Boolean",
            ]:
                return False
            elif data_type in [
                "System.String",
                "System.Type",
                "System.Guid",
                "System.DateTime",
            ] and not isinstance(criterion, str):
                return False
            elif (
                data_type == "System.Int32"
                and not isinstance(criterion, int)
                and not re.match(criterion, "[0-9]+")
            ):
                return False
            elif data_type == "System.Guid" and not re.match(
                criterion, "[a-z0-9]{8}(-[a-z0-9]{4}){3}-[a-z0-9]{12}"
            ):
                return False
            elif data_type == "System.DateTime":
                try:
                    valid_datetime = datetime.fromisoformat(criterion)
                except Exception:
                    return False
            elif data_type in "System.Boolean":
                if not isinstance(criterion, bool):
                    if isinstance(criterion, str):
                        if criterion.lower() not in [
                            "yes",
                            "no",
                            "on",
                            "off",
                            "true",
                            "false",
                        ]:
                            return False
                    else:
                        return False
            return True

    def validated_properties(self):
        base_table = self.base_table
        input_tables_set = set(
            [base_table]
            + list(self._input_filters.keys())
            + list(self._input_columns.keys())
        )
        properties = self.entity_properties(input_tables_set)
        unmatched_tables = [
            t for t in input_tables_set if not case_insensitive_key(properties, t)
        ]
        if unmatched_tables:
            self._module.fail_json(
                msg="Unable to look up table(s) {0}".format(unmatched_tables)
            )
        all_tables = list(properties.keys())
        for table in self._input_filters:
            real_table = [t for t in all_tables if t.lower() == table.lower()][0]
            unmatched_filters = [
                f
                for f in self._input_filters[table]
                if not case_insensitive_key(properties[real_table], f)
            ]
            if unmatched_filters:
                self._module.fail_json(
                    msg="Unable to look up column(s) {0} for table {1} specified in input filters".format(
                        unmatched_filters, real_table
                    )
                )
            for filter in [
                f for f in self._input_filters[table] if f is not None and f != []
            ]:
                data_type = case_insensitive_key(properties[real_table], filter)[0][
                    "Type"
                ]
                filter_content = self._input_filters[table][filter]
                if not self.valid_filter(data_type, filter_content):
                    self._module.fail_json(
                        msg="Filter criteria '{0}' not valid for property '{1}.{2}' with data type of '{3}'".format(
                            str(filter_content), real_table, filter, data_type
                        )
                    )
        for table in self._input_columns:
            real_table = [t for t in all_tables if t.lower() == table.lower()][0]
            unmatched_columns = [
                c
                for c in self._input_columns[table]
                if not case_insensitive_key(properties[real_table], c)
            ]
            if unmatched_columns:
                self._module.fail_json(
                    msg="Unable to look up column(s) {0} for table {1} specified in input columns".format(
                        unmatched_columns, real_table
                    )
                )
        return properties

    def relations(self, joined_tables):
        base_table = self.base_table
        try:
            table_filters = " ".join(
                [
                    ("OR " if i > 0 else "")
                    + "('{0}' IN (SourceType, TargetType) AND '{1}' IN (SourceType, TargetType))".format(
                        base_table, t
                    )
                    for i, t in enumerate(joined_tables)
                ]
            )
            # module.fail_json(
            #     msg="{0}\n{1}\n{2}".format(base_table, str(properties), table_filters)
            # )
            relations_res = self._client.query(
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

            table_relations = {}
            for table in joined_tables:
                table_relations[table] = {
                    r["TargetType"]: r
                    for r in relations_res["results"]
                    if r["SourceType"] == base_table and r["TargetType"] in table
                }

            unmatched_tables = [
                t for t in joined_tables if t not in table_relations.keys()
            ]
            if unmatched_tables:
                raise Exception(
                    "No relationship found between entities '{0}' and {1}".format(
                        base_table, str(unmatched_tables)
                    )
                )
            return table_relations

        except Exception as ex:
            self._module.fail_json(
                msg="Failed to retrieve table relationships between '{0}' and {1}: {2}".format(
                    base_table, str(joined_tables), str(ex)
                )
            )

    def metadata(self):
        """Retrieve properties, relationships, etc for tables being queried.
        Returns:
            a dictionary containing properties, relationships (joins), etc, for each table
        """
        base_table = self.base_table
        properties = self.validated_properties()
        all_tables = list(properties.keys())
        base_table = [t for t in all_tables if t.lower() == base_table.lower()][0]
        aliases = {
            t: "".join([u for u in t if u.isupper()]).lower() for t in all_tables
        }
        projected_columns = []
        for table in self._input_columns:
            real_table = [t for t in all_tables if t.lower() == table.lower()][0]
            if case_insensitive_key(properties, real_table):
                for column in self._input_columns[table]:
                    real_column = [
                        c for c in properties[real_table] if c.lower() == column.lower()
                    ][0]
                    if case_insensitive_key(properties[real_table], real_column):
                        projected_columns.append(
                            ".".join([aliases[real_table], real_column])
                        )

        joined_tables = [t for t in all_tables if t != base_table]
        relations = self.relations(joined_tables)

        metadata = dict(
            all_tables=all_tables,
            aliases=aliases,
            base_table=base_table,
            joined_tables=joined_tables,
            projected_columns=projected_columns,
            properties=properties,
            relations=relations,
        )
        return metadata

    def execute(self):
        metadata = self.metadata()
