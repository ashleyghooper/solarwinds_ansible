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

from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.sql_query_builder import (
    SQLQueryBuilder,
)

ORIONSDK_IMPORT_ERROR = None
try:
    from orionsdk import SwisClient
except ImportError:
    HAS_ORIONSDK = False
    ORIONSDK_IMPORT_ERROR = traceback.format_exc()
else:
    HAS_ORIONSDK = True


def case_insensitive_key(d, k):
    """Perform case-insensitive lookup in a dictionary for a key, and return list of values for all matching keys.

    Positional arguments:
    d -- dictionary in which to look up
    k -- key to look up
    """
    k = k.lower()
    return [d[key] for key in d if key.lower() == k]


class SolarWindsEntityAlias(Enum):
    """
    Map SolarWinds entity/table names to friendlier aliases. This enables use
    of alias names as inputs, and replacement of table names with aliases in
    outputs.
    """

    Agents = "Orion.AgentManagement.Agent"
    CustomProperties = "Orion.NodesCustomProperties"
    Engines = "Orion.Engines"
    Nodes = "Orion.Nodes"
    PollingEngines = "Orion.Engines"
    Volumes = "Orion.Volumes"

    @classmethod
    def from_alias_case_insensitive(cls, alias):
        entity = [m for m in cls.__members__.items() if m[0].lower() == alias.lower()][
            0
        ][0]
        return cls[entity].value


class SolarWindsQuery(object):
    """
    Class for facilitating dynamic SolarWinds Information Service queries.
    This is intended to enable automatic generation of a query according to
    the provided arguments, by querying the SolarWinds Information Service
    schema to determine the following:
    - cased column names
    - data types
    - tables and primary/foreign keys for joins between tables
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
        input_tables_set = set(
            [base_table]
            + [i for i in self._input_include]
            + [e for e in self._input_exclude]
            + [c for c in self._input_columns]
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

    def relations(self, suppl_tables):
        if not suppl_tables:
            return list()
        base_table = self._base_table
        try:
            table_filters = " ".join(
                [
                    ("OR " if i > 0 else "")
                    + "('{0}' IN (SourceType, TargetType) AND '{1}' IN (SourceType, TargetType))".format(
                        base_table, t
                    )
                    for i, t in enumerate(suppl_tables)
                ]
            )

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

            table_relations = {r["TargetType"]: r for r in relations_res["results"]}

            unmatched_tables = [
                t for t in suppl_tables if t not in table_relations.keys()
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
                    base_table, str(suppl_tables), str(ex)
                )
            )

    def metadata(self):
        """Retrieve properties, relationships, etc for tables being queried.
        Returns:
            a dictionary containing properties, relationships (joins), etc, for each table
        """
        properties = self.properties()
        all_tables = list(properties.keys())
        base_table = [t for t in all_tables if t.lower() == self._base_table.lower()][0]
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
                                projected_columns[real_table].append(real_column)
                    else:
                        projected_columns[real_table] = [
                            c for c in properties[real_table].keys()
                        ]

        if not projected_columns:
            projected_columns = {
                self._base_table: [c for c in properties[self._base_table].keys()]
            }

        suppl_tables = [t for t in all_tables if t != base_table]
        joined_tables = list(
            set(
                [t for t in self._include.keys() if t != base_table]
                if self._include is not None
                else [] + [t for t in self._exclude.keys() if t != base_table]
                if self._exclude is not None
                else []
            )
        )

        relations = self.relations(suppl_tables)

        base_join_columns = list(
            set(
                [
                    case_insensitive_key(properties[self._base_table], c)[0]["Name"]
                    for t in suppl_tables
                    for c in relations[t]["SourcePrimaryKeyNames"]
                    if relations[t]["SourceType"] == self._base_table
                ]
            )
        )

        join_columns = {
            t: list(
                set(
                    [
                        case_insensitive_key(properties[t], c)[0]["Name"]
                        for c in relations[t]["SourceForeignKeyNames"]
                    ]
                )
            )
            for t in suppl_tables
        }

        join_columns.update({self._base_table: base_join_columns})

        metadata = dict(
            all_tables=all_tables,
            aliases=aliases,
            base_table=base_table,
            join_columns=join_columns,
            joined_tables=joined_tables,
            suppl_tables=suppl_tables,
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
        queries = []
        base_query = (
            SQLQueryBuilder()
            .SELECT(
                *list(
                    set(
                        [
                            ".".join([metadata["aliases"][self._base_table], c])
                            for c in metadata["projected_columns"][self._base_table]
                            + metadata["join_columns"][self._base_table]
                        ]
                    )
                )
            )
            .FROM(
                "{0} AS {1}".format(
                    self._base_table, metadata["aliases"][self._base_table]
                )
            )
        )

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

        try:
            base_query_res = self._client.query(str(base_query))
            queries.append(str(base_query))
        except Exception as ex:
            self._module.fail_json(
                msg=(
                    "Query for base table '{0}' failed. Query: '{1}'. Exception: {2}".format(
                        self._base_table, str(base_query), str(ex)
                    )
                )
            )

        data = {}
        if "results" in base_query_res:
            results = {}
            results[self._base_table] = base_query_res["results"]
            data = [
                {
                    k: v
                    for k, v in sub.items()
                    if k in metadata["projected_columns"][self._base_table]
                }
                for sub in base_query_res["results"]
            ]
            indexed = {}
            for suppl_table in [
                t
                for t in metadata["suppl_tables"]
                if t in metadata["projected_columns"] and t in metadata["relations"]
            ]:
                relation = metadata["relations"][suppl_table]
                target_alias = metadata["aliases"][relation["TargetType"]]
                suppl_query = (
                    SQLQueryBuilder()
                    .SELECT(
                        *list(
                            set(
                                [
                                    ".".join([metadata["aliases"][suppl_table], c])
                                    for c in metadata["projected_columns"][suppl_table]
                                    + metadata["join_columns"][suppl_table]
                                ]
                            )
                        )
                    )
                    .FROM(
                        "{0} AS {1}".format(
                            suppl_table, metadata["aliases"][suppl_table]
                        )
                    )
                )

                key_sets = []
                for k in range(len(relation["SourcePrimaryKeyNames"])):
                    try:
                        key_sets.append(
                            set(
                                [
                                    # Elements should be string
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
                    except Exception as ex:
                        self._module.fail_json(
                            msg=str(
                                "Failed to look up foreign key [{0}] to join base table '{1}' to supplemental table '{2}' : {3}".format(
                                    k + 1, self._base_table, suppl_table, ex
                                )
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
                        msg="Join query for supplemental table '{0}' failed. Query: '{1}'. Exception: {2}".format(
                            suppl_table, str(suppl_query), str(ex)
                        )
                    )

                results[suppl_table] = join_query_res["results"]

                indexed[suppl_table] = {}
                for r in results[suppl_table]:
                    keys = []
                    for k in range(len(relation["SourcePrimaryKeyNames"])):
                        try:
                            keys.append(
                                # Elements should be string
                                str(
                                    case_insensitive_key(
                                        r,
                                        relation["SourcePrimaryKeyNames"][k],
                                    )[0]
                                )
                            )
                        except Exception as ex:
                            self._module.fail_json(
                                msg=str(
                                    "Failed to look up primary key [{0}] to join data from supplemental table '{1}' back to base table '{2}' : {3}".format(
                                        k + 1, suppl_table, self._base_table, ex
                                    )
                                )
                            )
                    if not tuple(keys) in indexed[suppl_table]:
                        indexed[suppl_table][tuple(keys)] = []
                    indexed[suppl_table][tuple(keys)].append(r)

            for i, r in enumerate(base_query_res["results"]):
                for joined_table in [
                    t
                    for t in metadata["suppl_tables"]
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
                    try:
                        suppl_table_name = SolarWindsEntityAlias(suppl_table).name
                    except Exception:
                        suppl_table_name = suppl_table
                    if tuple(keys) in indexed[suppl_table]:
                        data[i][suppl_table_name] = [
                            {
                                k: v
                                for k, v in sub.items()
                                if k in metadata["projected_columns"][joined_table]
                            }
                            for sub in indexed[suppl_table][tuple(keys)]
                        ]

        info = {
            "data": data,
            "count": len(data),
            "queries": str(queries),
        }
        return info
