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
    Map SolarWinds table names to shorter/friendlier entity names. This enables
    use of entitty names as inputs, and replacement of table names with entity
    names in outputs.
    """

    Agents = "Orion.AgentManagement.Agent"
    CustomProperties = "Orion.NodesCustomProperties"
    Engines = "Orion.Engines"
    Interfaces = "Orion.NPM.Interfaces"
    Nodes = "Orion.Nodes"
    PollingEngines = "Orion.Engines"
    Status = "Orion.StatusInfo"
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

    def table_name(self, value):
        """
        Look up provided value in the SolarWindsEntityAlias enum to resolve it
        to a table name.
        """
        try:
            entity_name = SolarWindsEntityAlias.from_alias_case_insensitive(value)
        except Exception:
            entity_name = value
        return entity_name

    def __init__(self, module, solarwinds_client):
        self._module = module
        self._client = solarwinds_client

    def query(
        self,
        input_base_table,
        input_columns,
        input_includes,
        input_excludes,
    ):
        inputs = {
            "base_table": self.table_name(input_base_table),
            "columns": {self.table_name(t): input_columns[t] for t in input_columns}
            if input_columns is not None
            else {},
            "includes": {self.table_name(t): input_includes[t] for t in input_includes}
            if input_includes is not None
            else {},
            "excludes": {self.table_name(t): input_excludes[t] for t in input_excludes}
            if input_excludes is not None
            else {},
        }
        self._metadata = self.metadata(inputs)
        # Set local variables from metadata for convenience
        metadata = self._metadata
        aliases = metadata["aliases"]
        all_tables = metadata["all_tables"]
        base_table = metadata["base_table"]
        joined_tables = metadata["joined_tables"]
        params = metadata["params"]
        projected_columns = metadata["projected_columns"]
        relations = metadata["relations"]
        suppl_tables = metadata["suppl_tables"]

        queries = []
        base_query = (
            SQLQueryBuilder()
            .SELECT(*self.projected_columns(base_table))
            .FROM(
                "{0} AS {1}".format(
                    base_table,
                    aliases[base_table],
                )
            )
        )

        for table in joined_tables:
            table_relation = relations[table]
            source_type = table_relation["SourceType"]
            target_type = table_relation["TargetType"]
            join_columns = [
                (" AND " if i > 0 else "")
                + "{0}.{1} = {2}.{3}".format(
                    aliases[target_type],
                    table_relation["SourceForeignKeyNames"][i],
                    aliases[source_type],
                    table_relation["SourcePrimaryKeyNames"][i],
                )
                for i in range(len(table_relation["SourceForeignKeyNames"]))
            ]
            base_query.INNER_JOIN(
                "{0} AS {1} ON {2}".format(table, aliases[table], "".join(join_columns))
            )

        # Add filters
        if "includes" in params and params["includes"]:
            base_query.WHERE(self.where_clause(params["includes"], all_tables))
        if "excludes" in params and params["excludes"]:
            base_query.WHERE(
                "NOT {0}".format(self.where_clause(params["excludes"], all_tables))
            )

        try:
            queries.append(str(base_query))
            base_query_res = self._client.query(str(base_query))
        except Exception as ex:
            self._module.fail_json(
                msg=(
                    "Query for base table '{0}' failed. Queries: {1}. Exception: {2}".format(
                        base_table, str(queries), str(ex)
                    )
                )
            )

        data = {}
        if "results" in base_query_res and base_query_res["results"]:
            results = {}
            results[base_table] = base_query_res["results"]
            data = [
                {k: v for k, v in sub.items() if k in projected_columns[base_table]}
                for sub in base_query_res["results"]
            ]
            indexed = {}
            for suppl_table in [
                t for t in suppl_tables if t in projected_columns and t in relations
            ]:
                relation = relations[suppl_table]
                suppl_query = (
                    SQLQueryBuilder()
                    .SELECT(*self.projected_columns(suppl_table))
                    .FROM("{0} AS {1}".format(suppl_table, aliases[suppl_table]))
                )

                key_sets = []
                for k in range(len(relation["SourceForeignKeyNames"])):
                    try:
                        key_sets.append(
                            set(
                                [
                                    # Elements should be string
                                    str(
                                        case_insensitive_key(
                                            r,
                                            relation["SourcePrimaryKeyNames"][k],
                                        )[0]
                                    )
                                    for r in results[base_table]
                                ]
                            )
                        )
                    except Exception as ex:
                        self._module.fail_json(
                            msg=str(
                                "Failed to look up primary key column [{0}] to join base table '{1}' to supplemental table '{2}'. Relation: {3}. Queries: {4}. Exception: {5}".format(
                                    k + 1,
                                    base_table,
                                    suppl_table,
                                    str(relation),
                                    str(queries),
                                    str(ex),
                                )
                            )
                        )

                    # Apply relevant filters to nested records
                    if (
                        "includes" in params
                        and params["includes"]
                        and suppl_table in params["includes"]
                    ):
                        suppl_query.WHERE(
                            self.where_clause(params["includes"], [suppl_table])
                        )
                    if (
                        "excludes" in params
                        and params["excludes"]
                        and suppl_table in params["excludes"]
                    ):
                        suppl_query.WHERE(
                            self.where_clause(params["excludes"], [suppl_table])
                        )

                    # Join to rows in base table
                    suppl_query.WHERE(
                        "{0} IN ({1})".format(
                            ".".join(
                                [
                                    aliases[relation["TargetType"]],
                                    relation["SourceForeignKeyNames"][k],
                                ]
                            ),
                            ", ".join(list(key_sets[k])),
                        )
                    )

                try:
                    queries.append(str(suppl_query))
                    join_query_res = self._client.query(str(suppl_query))
                except Exception as ex:
                    self._module.fail_json(
                        msg="Join query for supplemental table '{0}' failed. Queries: {1}. Exception: {2}".format(
                            suppl_table, str(queries), str(ex)
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
                                        relation["SourceForeignKeyNames"][k],
                                    )[0]
                                )
                            )
                        except Exception as ex:
                            self._module.fail_json(
                                msg=str(
                                    "Failed to look up foreign key column [{0}] to join data from supplemental table '{1}' back to base table '{2}'. Relation: {3}. Queries: {4}. Exception: {5}".format(
                                        k + 1,
                                        suppl_table,
                                        base_table,
                                        str(relation),
                                        str(queries),
                                        str(ex),
                                    )
                                )
                            )
                    if not tuple(keys) in indexed[suppl_table]:
                        indexed[suppl_table][tuple(keys)] = []
                    indexed[suppl_table][tuple(keys)].append(r)

            # self._module.fail_json(msg=str(indexed))

            for i, r in enumerate(base_query_res["results"]):
                for suppl_table in [
                    t for t in suppl_tables if t in projected_columns and t in relations
                ]:
                    relation = relations[suppl_table]
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
                                if k in projected_columns[suppl_table]
                            }
                            for sub in indexed[suppl_table][tuple(keys)]
                        ]

        info = {
            "data": data,
            "count": len(data),
            "queries": str(queries),
        }
        return info

    def metadata(self, inputs):
        """Retrieve properties, relationships, etc for tables being queried.
        Returns:
            a dictionary containing properties, relationships (joins), etc, for each table
        """
        properties = self.entity_properties(inputs)
        all_tables = list(properties.keys())
        params = self.params(properties, all_tables, inputs)
        base_table = [
            t for t in all_tables if t.lower() == inputs["base_table"].lower()
        ][0]
        aliases = {
            t: "_".join([u for u in t if u.isupper()]).lower() for t in all_tables
        }
        projected_columns = {}
        if inputs["columns"]:
            for table in inputs["columns"]:
                real_table = [t for t in all_tables if t.lower() == table.lower()][0]
                projected_columns[real_table] = []
                if case_insensitive_key(properties, real_table):
                    if inputs["columns"][table] is not None:
                        for column in inputs["columns"][table]:
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
            projected_columns = {base_table: [c for c in properties[base_table].keys()]}

        suppl_tables = [t for t in all_tables if t != base_table]
        joined_tables = list(
            set(
                (
                    [i for i in params["includes"].keys() if i != base_table]
                    if "includes" in params and params["includes"] is not None
                    else []
                )
                + (
                    [e for e in params["excludes"].keys() if e != base_table]
                    if "excludes" in params and params["excludes"] is not None
                    else []
                )
            )
        )

        relations = self.relations(base_table, suppl_tables)

        base_join_columns = list(
            set(
                [
                    case_insensitive_key(properties[base_table], c)[0]["Name"]
                    for t in suppl_tables
                    for c in relations[t]["SourcePrimaryKeyNames"]
                    if relations[t]["SourceType"] == base_table
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

        join_columns.update({base_table: base_join_columns})

        metadata = dict(
            all_tables=all_tables,
            aliases=aliases,
            base_table=base_table,
            join_columns=join_columns,
            joined_tables=joined_tables,
            params=params,
            projected_columns=projected_columns,
            properties=properties,
            relations=relations,
            suppl_tables=suppl_tables,
        )
        return metadata

    def projected_columns(self, table):
        return list(
            set(
                [
                    ".".join([self._metadata["aliases"][table], c])
                    for c in self._metadata["projected_columns"][table]
                    + self._metadata["join_columns"][table]
                ]
            )
        )

    def entity_properties(self, inputs):
        """
        Query, validate, and return all entity properties from SWIS metadata.
        """
        base_table = inputs["base_table"]
        input_tables_set = set(
            [base_table]
            + [i for i in inputs["includes"]]
            + [e for e in inputs["excludes"]]
            + [c for c in inputs["columns"]]
        )
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
        except Exception as ex:
            self._module.fail_json(
                msg="Failed to retrieve entity properties for {0}: {1}".format(
                    str(input_tables_set), str(ex)
                )
            )

        unmatched_tables = [
            t for t in input_tables_set if not case_insensitive_key(properties, t)
        ]
        if unmatched_tables:
            self._module.fail_json(
                msg="Unable to look up table(s) {0}".format(unmatched_tables)
            )
        return properties

    def params(self, properties, all_tables, inputs):
        """
        Parse and validate inputs, resolving names and aliases to SWIS names.
        """
        params = {}
        params["includes"] = self.validated_filters(
            properties, all_tables, inputs["includes"]
        )
        params["excludes"] = self.validated_filters(
            properties, all_tables, inputs["excludes"]
        )
        params["columns"] = self.validated_columns(
            properties, all_tables, inputs["columns"]
        )
        return params

    def validated_filters(self, properties, all_tables, inputs):
        """
        Iterate over filter inputs and verify tables and columns.
        """
        params = {}
        for table in [t for t in inputs if inputs[t] is not None]:
            real_table = [t for t in all_tables if t.lower() == table.lower()][0]
            params[real_table] = {}
            unmatched_includes = [
                i
                for i in inputs[table]
                if not case_insensitive_key(properties[real_table], i)
            ]
            if unmatched_includes:
                self._module.fail_json(
                    msg="Unable to look up column(s) {0} for table {1} specified in include filters".format(
                        unmatched_includes, real_table
                    )
                )
            for filter in [f for f in inputs[table] if f is not None and f != []]:
                real_column = [
                    c for c in properties[real_table] if c.lower() == filter.lower()
                ][0]
                params[real_table][real_column] = inputs[table][filter]
        return params

    def validated_columns(self, properties, all_tables, inputs):
        """
        Iterate over columns inputs and verify tables and columns.
        """
        params = {}
        for table in inputs:
            real_table = [t for t in all_tables if t.lower() == table.lower()][0]
            params[real_table] = []
            if inputs[table] is not None:
                unmatched_columns = [
                    c
                    for c in inputs[table]
                    if not case_insensitive_key(properties[real_table], c)
                ]
                if unmatched_columns:
                    self._module.fail_json(
                        msg="Unable to look up column(s) {0} for table {1} specified in input columns".format(
                            unmatched_columns, real_table
                        )
                    )
                for column in inputs[table]:
                    real_column = [
                        c for c in properties[real_table] if c.lower() == column.lower()
                    ][0]
                    params[real_table].append(real_column)
            else:
                params[real_table] = properties[real_table]
        return params

    def relations(self, base_table, suppl_tables):
        if not suppl_tables:
            return list()
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

            # Add relation between Orion.Nodes and Orion.StatusInfo that is not
            # represented in Metadata.Relationship.

            table_relations = {
                "Orion.StatusInfo": {
                    "SourceType": "Orion.Nodes",
                    "TargetType": "Orion.StatusInfo",
                    "SourcePrimaryKeyNames": ["Status"],
                    "SourceForeignKeyNames": ["StatusId"],
                    "SourceCardinalityMin": "1",
                    "SourceCardinalityMax": "1",
                    "TargetPrimaryKeyNames": [],
                    "TargetForeignKeyNames": [],
                    "TargetCardinalityMin": "0",
                    "TargetCardinalityMax": "*",
                }
            }

            table_relations.update(
                {r["TargetType"]: r for r in relations_res["results"]}
            )

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
                    "System.Single",
                    "System.Double",
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
                        criterion, "\d+"
                    ):
                        return False
                    else:
                        filters.append(("=", criterion))
                elif data_type in ["System.Single", "System.Double"]:
                    if not isinstance(criterion, float) and not re.match(
                        criterion,
                        "\d+(\.\d+)?",
                    ):
                        return False
                    else:
                        filters.append(("=", criterion))
                elif data_type == "System.Guid":
                    if not re.match(
                        criterion, "[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}"
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

    def where_clause(self, filters, tables):
        if filters is not None:
            for table in [t for t in tables if t in filters]:
                alias = self._metadata["aliases"][table]
                for column in filters[table]:
                    filter_content = filters[table][column]
                    data_type = self._metadata["properties"][table][column]["Type"]
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

                    return "({0})".format(column_criteria_sql)
