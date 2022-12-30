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
from enum import Enum

from ansible.module_utils.basic import missing_required_lib

from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.query_builder import (
    QueryBuilder,
)

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


class SolarWindsClient(object):
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


class SolarWindsEntityAlias(Enum):
    """
    Map SolarWinds entity/table names to friendlier aliases.
    """

    Agents = "Orion.AgentManagement.Agent"
    CustomProperties = "Orion.NodesCustomProperties"
    Engines = "Orion.Engines"
    Nodes = "Orion.Nodes"
    PollingEngines = "Orion.Engines"
    Volumes = "Orion.Volumes"

    @classmethod
    def from_alias_case_insensitive(cls, alias):
        # return cls.__members__.items()
        entity = [m for m in cls.__members__.items() if m[0].lower() == alias.lower()][
            0
        ][0]
        return cls[entity].value


class SolarWindsQuery(object):
    """
    Class for facilitating dynamic SolarWinds Information Service queries.
    """

    @property
    def base_table(self):
        return self._base_table

    @property
    def input_include(self):
        return self._input_include

    @property
    def input_exclude(self):
        return self._input_exclude

    @property
    def input_columns(self):
        return self._input_columns

    @base_table.setter
    def base_table(self, value):
        try:
            self._base_table = SolarWindsEntityAlias.from_alias_case_insensitive(value)
        except Exception:
            self._base_table = value

    @input_include.setter
    def input_include(self, value=dict()):
        self._input_include = {}
        if value:
            for input_table in value:
                try:
                    table = SolarWindsEntityAlias.from_alias_case_insensitive(
                        input_table
                    )
                except Exception:
                    table = input_table
                self._input_include[table] = value[input_table]

    @input_exclude.setter
    def input_exclude(self, value=dict()):
        self._input_exclude = {}
        if value:
            for input_table in value:
                try:
                    table = SolarWindsEntityAlias.from_alias_case_insensitive(
                        input_table
                    )
                except Exception:
                    table = input_table
                self._input_exclude[table] = value[input_table]

    @input_columns.setter
    def input_columns(self, value):
        self._input_columns = {}
        if value:
            for input_table in value:
                try:
                    table = SolarWindsEntityAlias.from_alias_case_insensitive(
                        input_table
                    )
                except Exception:
                    table = input_table
                self._input_columns[table] = value[input_table]

    def __init__(self, module, solarwinds_client):
        self._module = module
        self._client = solarwinds_client
        self._base_table = None
        self._input_include = None
        self._input_exclude = None
        self._input_columns = None
        self._include = None
        self._exclude = None
        self._columns = None

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

    def properties(self):
        base_table = self._base_table
        # self._module.fail_json(str(self._input_filters))
        # input_filters_set = set(
        #     [i for i in self._input_include] + [e for e in self._input_exclude]
        # )
        input_tables_set = set(
            [base_table]
            + [i for i in self._input_include]
            + [e for e in self._input_exclude]
            + [c for c in self._input_columns]
        )
        # self._module.fail_json(str(input_tables_set))
        properties = self.entity_properties(input_tables_set)
        unmatched_tables = [
            t for t in input_tables_set if not case_insensitive_key(properties, t)
        ]
        if unmatched_tables:
            self._module.fail_json(
                msg="Unable to look up table(s) {0}".format(unmatched_tables)
            )
        all_tables = list(properties.keys())
        for table in [
            t for t in self._input_include if self._input_include[t] is not None
        ]:
            real_table = [t for t in all_tables if t.lower() == table.lower()][0]
            if not self._include:
                self._include = {}
            self._include[real_table] = {}
            unmatched_includes = [
                i
                for i in self._input_include[table]
                if not case_insensitive_key(properties[real_table], i)
            ]
            if unmatched_includes:
                self._module.fail_json(
                    msg="Unable to look up column(s) {0} for table {1} specified in include filters".format(
                        unmatched_includes, real_table
                    )
                )
            for filter in [
                f for f in self._input_include[table] if f is not None and f != []
            ]:
                real_column = [
                    c for c in properties[real_table] if c.lower() == filter.lower()
                ][0]
                self._include[real_table][real_column] = self._input_include[table][
                    filter
                ]

        for table in [
            t for t in self._input_exclude if self._input_exclude[t] is not None
        ]:
            real_table = [t for t in all_tables if t.lower() == table.lower()][0]
            if not self._exclude:
                self._exclude = {}
            self._exclude[real_table] = {}
            unmatched_excludes = [
                e
                for e in self._input_exclude[table]
                if not case_insensitive_key(properties[real_table], e)
            ]
            if unmatched_excludes:
                self._module.fail_json(
                    msg="Unable to look up column(s) {0} for table {1} specified in exclude filters".format(
                        unmatched_excludes, real_table
                    )
                )
            for filter in [
                f for f in self._input_exclude[table] if f is not None and f != []
            ]:
                real_column = [
                    c for c in properties[real_table] if c.lower() == filter.lower()
                ][0]
                self._exclude[real_table][real_column] = self._input_exclude[table][
                    filter
                ]

        for table in self._input_columns:
            real_table = [t for t in all_tables if t.lower() == table.lower()][0]
            if not self._columns:
                self._columns = {}
            self._columns[real_table] = []
            if self._input_columns[table] is not None:
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
                for column in self._input_columns[table]:
                    real_column = [
                        c for c in properties[real_table] if c.lower() == column.lower()
                    ][0]
                    self._columns[real_table].append(real_column)
            else:
                self._columns[real_table] = properties[real_table]
        return properties

    def relations(self, joined_tables):
        if not joined_tables:
            return list()
        base_table = self._base_table
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

            table_relations = {
                r["TargetType"]: r
                for r in relations_res["results"]
                # if r["SourceType"] == base_table
            }
            # table_relations = {}
            # for table in joined_tables:
            #     table_relations[table] = {
            #         r["TargetType"]: r
            #         for r in relations_res["results"]
            #         if r["SourceType"] == base_table and r["TargetType"] in table
            #     }

            unmatched_tables = [
                t for t in joined_tables if t not in table_relations.keys()
            ]
            if unmatched_tables:
                raise Exception(
                    "No relationship found between entities '{0}' and {1}".format(
                        base_table, str(unmatched_tables)
                    )
                )
            # self._module.fail_json(msg=table_relations)
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
        base_table = self._base_table
        properties = self.properties()
        all_tables = list(properties.keys())
        base_table = [t for t in all_tables if t.lower() == base_table.lower()][0]
        aliases = {
            t: "_".join([u for u in t if u.isupper()]).lower() for t in all_tables
        }
        projected_columns = {}
        if self._input_columns:
            for table in self._input_columns:
                real_table = [t for t in all_tables if t.lower() == table.lower()][0]
                projected_columns[real_table] = []
                if case_insensitive_key(properties, real_table):
                    if self._input_columns[table] is not None:
                        for column in self._input_columns[table]:
                            real_column = [
                                c
                                for c in properties[real_table]
                                if c.lower() == column.lower()
                            ][0]
                            if case_insensitive_key(
                                properties[real_table], real_column
                            ):
                                projected_columns[real_table].append(
                                    ".".join([aliases[real_table], real_column])
                                )
                    else:
                        # self._module.fail_json(
                        #     msg=str(
                        #         [
                        #             ".".join([aliases[real_table], c])
                        #             for t in projected_columns
                        #             for c in properties[real_table]
                        #             if c not in t
                        #         ]
                        #     )
                        # )
                        projected_columns[real_table] = [
                            ".".join([aliases[real_table], c])
                            for c in properties[real_table].keys()
                        ]

        if not projected_columns:
            # self._module.fail_json(msg=str(properties[self._base_table]))
            projected_columns = {
                self._base_table: [
                    ".".join([aliases[self._base_table], c])
                    for c in properties[self._base_table].keys()
                ]
            }
            # projected_columns = {
            #     c: dict(alias=aliases[real_table], table=real_table)
            #     for c in properties[base_table]
            # }
        # self._module.fail_json(msg=str(projected_columns))

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

    def column_filters(self, data_type, filter_content):
        filters = []
        if isinstance(filter_content, dict):
            modifiers = filter_content.keys()
            for key in modifiers:
                modifier_filters = None
                if key in ["max", "min"]:
                    modifier_filters = self.column_filters(
                        data_type, filter_content[key]
                    )
                if not modifier_filters:
                    return False
                else:
                    if key == "min":
                        modifier = ">="
                    elif key == "max":
                        modifier = "<="
                    else:
                        return False
                    filters.append((modifier, modifier_filters[0][1]))
        else:
            if isinstance(filter_content, list):
                criteria_list = filter_content
            else:
                criteria_list = [filter_content]
            for criterion in [c for c in criteria_list if c is not None]:
                if data_type not in [
                    "System.String",
                    "System.Type",
                    "System.Guid",
                    "System.DateTime",
                    "System.Int16",
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
                elif data_type == "System.String":
                    filters.append(("LIKE", "'{0}'".format(criterion)))
                elif data_type in ["System.Int16", "System.Int32"]:
                    if not isinstance(criterion, int) and not re.match(
                        criterion, "[0-9]+"
                    ):
                        return False
                    else:
                        filters.append(("=", criterion))
                elif data_type == "System.Guid":
                    if not re.match(
                        criterion, "[a-z0-9]{8}(-[a-z0-9]{4}){3}-[a-z0-9]{12}"
                    ):
                        return False
                    else:
                        filters.append(("=", "'{0}'".format(criterion)))
                elif data_type == "System.DateTime":
                    try:
                        datetime.fromisoformat(criterion)
                    except Exception:
                        return False
                    filters.append(("=", "'{0}'".format(criterion)))
                elif data_type in "System.Boolean":
                    if isinstance(criterion, bool):
                        filters.append(("=", criterion))
                    elif isinstance(criterion, str):
                        if criterion.lower() in [
                            "yes",
                            "on",
                            "true",
                        ]:
                            filters.append(("=", True))
                        elif criterion.lower() in [
                            "no",
                            "off",
                            "false",
                        ]:
                            filters.append(("=", False))
                        else:
                            return False
                else:
                    # Unhandled data type
                    return False
        return filters

    def execute(self):
        metadata = self.metadata()
        # self._module.fail_json(msg=str(metadata))
        queries = []
        base_query = (
            QueryBuilder()
            .SELECT(
                *[
                    ".".join([metadata["aliases"][self._base_table], c])
                    for c in metadata["properties"][self._base_table]
                ]
            )
            .FROM(
                "{0} AS {1}".format(
                    self._base_table, metadata["aliases"][self._base_table]
                )
            )
        )

        # Add joins for secondary tables
        for table in metadata["joined_tables"]:
            table_relation = metadata["relations"][table]
            source_type = table_relation["SourceType"]
            target_type = table_relation["TargetType"]
            alias = metadata["aliases"][table]
            join_columns = [
                (" AND " if i > 0 else "")
                + "{0}.{1} = {2}.{3}".format(
                    metadata["aliases"][target_type],
                    table_relation["SourceForeignKeyNames"][i],
                    metadata["aliases"][source_type],
                    table_relation["SourcePrimaryKeyNames"][i],
                )
                for i in range(len(table_relation["SourceForeignKeyNames"]))
            ]
            base_query.INNER_JOIN(
                "{0} AS {1} ON {2}".format(table, alias, "".join(join_columns))
            )

        # Add include filters
        if self._include:
            for table in [t for t in metadata["all_tables"] if t in self._include]:
                alias = metadata["aliases"][table]
                for column in self._include[table]:
                    filter_content = self._include[table][column]
                    data_type = metadata["properties"][table][column]["Type"]
                    column_filters = self.column_filters(data_type, filter_content)
                    # self._module.fail_json(msg=column_criteria)
                    if not column_filters:
                        self._module.fail_json(
                            msg="Filter criteria '{0}' not valid for property '{1}.{2}' with data type of '{3}'".format(
                                str(filter_content), table, column, data_type
                            )
                        )
                    column_criteria_sql = " ".join(
                        [
                            ("OR " if i > 0 else "")
                            + "{0}.{1} {2} {3}".format(alias, column, c[0], c[1])
                            for i, c in enumerate(column_filters)
                        ]
                    )

                    base_query.WHERE("({0})".format(column_criteria_sql))

        # Add exclude filters
        if self._exclude:
            for table in [t for t in metadata["all_tables"] if t in self._exclude]:
                alias = metadata["aliases"][table]
                for column in self._exclude[table]:
                    filter_content = self._exclude[table][column]
                    data_type = metadata["properties"][table][column]["Type"]
                    column_filters = self.column_filters(data_type, filter_content)
                    # self._module.fail_json(msg=column_criteria)
                    if not column_filters:
                        self._module.fail_json(
                            msg="Filter criteria '{0}' not valid for property '{1}.{2}' with data type of '{3}'".format(
                                str(filter_content), table, column, data_type
                            )
                        )
                    column_criteria_sql = " ".join(
                        [
                            ("OR " if i > 0 else "")
                            + "{0}.{1} {2} {3}".format(alias, column, c[0], c[1])
                            for i, c in enumerate(column_filters)
                        ]
                    )

                    base_query.WHERE("NOT ({0})".format(column_criteria_sql))

        # self._module.fail_json(msg=str(query))

        try:
            base_query_res = self._client.query(str(base_query))
            queries.append(str(base_query))
        except Exception as ex:
            self._module.fail_json(
                msg="Base query for table '{0}' failed: {1}".format(
                    self._base_table, str(ex)
                )
            )

        data = []
        if "results" in base_query_res:
            results = {}
            results[self._base_table] = base_query_res["results"]
            indexed = {}
            data = results[self._base_table]
            for joined_table in [
                t
                for t in metadata["joined_tables"]
                if t in metadata["projected_columns"] and t in metadata["relations"]
            ]:
                relation = metadata["relations"][joined_table]
                target_alias = metadata["aliases"][relation["TargetType"]]
                suppl_query = (
                    QueryBuilder()
                    .SELECT(*metadata["projected_columns"][joined_table])
                    .FROM(
                        "{0} AS {1}".format(
                            joined_table, metadata["aliases"][joined_table]
                        )
                    )
                )

                key_sets = []
                for k in range(len(relation["SourcePrimaryKeyNames"])):
                    key_sets.append(
                        set(
                            [
                                str(
                                    case_insensitive_key(
                                        r,
                                        relation["SourceForeignKeyNames"][k],
                                    )[0]
                                )
                                for r in results[self._base_table]
                            ]
                        )
                    )
                    suppl_query.WHERE(
                        "{0} IN ({1})".format(
                            ".".join(
                                [target_alias, relation["SourceForeignKeyNames"][k]]
                            ),
                            ", ".join(list(key_sets[k])),
                        )
                    )

                try:
                    join_query_res = self._client.query(str(suppl_query))
                    queries.append(str(suppl_query))
                except Exception as ex:
                    self._module.fail_json(
                        msg="Join query for joined table {0} failed: {1}".format(
                            joined_table, str(ex)
                        )
                    )

                results[joined_table] = join_query_res["results"]

                indexed[joined_table] = {}
                for r in results[joined_table]:
                    keys = []
                    for k in range(len(relation["SourcePrimaryKeyNames"])):
                        keys.append(
                            str(
                                case_insensitive_key(
                                    r,
                                    relation["SourcePrimaryKeyNames"][k],
                                )[0]
                            )
                        )
                    if not tuple(keys) in indexed[joined_table]:
                        indexed[joined_table][tuple(keys)] = []
                    indexed[joined_table][tuple(keys)].append(r)

            # self._module.fail_json(msg=str(indexed))

            for i, r in enumerate(data):
                for joined_table in [
                    t
                    for t in metadata["joined_tables"]
                    if t in metadata["projected_columns"] and t in metadata["relations"]
                ]:
                    relation = metadata["relations"][joined_table]
                    keys = []
                    for k in range(len(relation["SourcePrimaryKeyNames"])):
                        keys.append(
                            str(
                                case_insensitive_key(
                                    r,
                                    relation["SourcePrimaryKeyNames"][k],
                                )[0]
                            )
                        )
                        # keys.append(r[relation["SourcePrimaryKeyNames"][k]])
                    try:
                        suppl_table_name = SolarWindsEntityAlias(joined_table).name
                    except Exception:
                        suppl_table_name = joined_table
                    if tuple(keys) in indexed[joined_table]:
                        data[i][suppl_table_name] = indexed[joined_table][tuple(keys)]

            # self._module.fail_json(msg=str(data))

        info = {
            "data": data,
            "count": len(data),
            "queries": str(queries),
        }
        return info

    #             # for base_record in data:
    #             # relation = metadata["relations"][joined_table]
    #             # self._module.fail_json(msg=str(relation["SourcePrimaryKeyNames"]))

    #             primary_keys = relation["SourcePrimaryKeyNames"]
    #             foreign_keys = relation["SourceForeignKeyNames"]

    #             # node_ids = [[i["NodeID"] for i in query_results]

    #             #     i[relation["SourcePrimaryKeyNames"][k]]
    #             #     for i in query_results
    #             #     for k in relation["SourcePrimaryKeyNames"]
    #             # ]
    #             self._module.fail_json(msg=str(join_ids))
    #         node_custom_properties = self.node_custom_properties(module, node_ids)
    #         if node_custom_properties is not None:
    #             node_custom_properties_indexed = {
    #                 ncp["NodeID"]: ncp for ncp in node_custom_properties
    #             }
    #         else:
    #             node_custom_properties_indexed = {}
    #         node_agents = self.node_agents(module, node_ids)
    #         if node_agents is not None:
    #             node_agents_indexed = {na["NodeID"]: na for na in node_agents}
    #         else:
    #             node_agents_indexed = {}

    #         nodes = []
    #         for node_data in base_query_data:
    #             if node_data["NodeID"] in node_custom_properties_indexed:
    #                 node_data.update(
    #                     {
    #                         "CustomProperties": node_custom_properties_indexed[
    #                             node_data["NodeID"]
    #                         ]
    #                     }
    #                 )
    #             if node_data["NodeID"] in node_agents_indexed:
    #                 node_data.update(
    #                     {"Agent": node_agents_indexed[node_data["NodeID"]]}
    #                 )

    #             nodes.append(node_data)

    #     else:
    #         nodes = None

    #     info = {
    #         "data": base_query_res["results"],
    #         "count": len(base_query_res["results"]),
    #         "query": str(base_query),
    #     }
    #     return info

    #     # for criterion in [c for c in column_criteria if c is not None]:

    # #                     if column_criteria:
    # #                         column_criteria = " ".join([column_criteria, "OR"])
    # #                     column_criteria += " ".join(
    # #                         [
    # #                             ".".join([alias, column]),
    # #                             comparator,
    # #                             criterion,
    # #                         ]
    # #                     )

    # #                     query.WHERE("({0})".format(column_criteria))

    # #                 # Iterate over each value for the current param and validate
    # #                 for element in [v for v in param_value if v is not None]:
    # #                     match = None
    # #                     comparator = "LIKE"
    # #                     wrap = "'"
    # #                     if (
    # #                         hasattr(table_class, "boolean_columns")
    # #                         and column in table_class.boolean_columns
    # #                     ):
    # #                         comparator = "="
    # #                         if isinstance(element, bool):
    # #                             match = str(element)
    # #                         elif isinstance(element, str):
    # #                             match = (
    # #                                 "True"
    # #                                 if str(element).lower() in ["yes", "on", "true"]
    # #                                 else "False"
    # #                             )
    # #                         else:
    # #                             module.fail_json(
    # #                                 msg="All filters on column '{0}' should be boolean: {1}".format(
    # #                                     column
    # #                                 )
    # #                             )
    # #                     elif isinstance(criteria, dict):
    # #                         column = element
    # #                         match = param_value[element]
    # #                     elif isinstance(criteria, str):
    # #                         match = element
    # #                     elif isinstance(criteria, int):
    # #                         wrap = None
    # #                         try:
    # #                             match = int(element)
    # #                         except Exception as ex:
    # #                             pass
    # #                     else:
    # #                         match = element

    # #                     if not match:
    # #                         module.fail_json(
    # #                             msg="Invalid filter for column '{0}'".format(column)
    # #                         )

    # #                     if wrap:
    # #                         criterion = "{0}{1}{2}".format(wrap, match, wrap)
    # #                     else:
    # #                         criterion = str(match)

    # #                     if column_criteria:
    # #                         column_criteria = " ".join([column_criteria, "OR"])
    # #                     column_criteria += " ".join(
    # #                         [
    # #                             ".".join([alias, column]),
    # #                             comparator,
    # #                             criterion,
    # #                         ]
    # #                     )

    # #                     query.WHERE("({0})".format(column_criteria))

    # # # module.fail_json(msg="{0}".format(str(query)))

    # # # from_where = [" ".join(["FROM", " ".join(tables_spec)])]
    # # # if len(criteria.strip()) > 0:
    # # #     from_where.append(" ".join(["WHERE", criteria]))

    # # # Assemble and run the query
    # # # query = " ".join(["SELECT", projection] + from_where)

    # # try:
    # #     query_res = self.solarwinds.client.query(str(query))
    # # except Exception as ex:
    # #     module.fail_json(msg="Nodes query failed: {0}".format(str(ex)))
