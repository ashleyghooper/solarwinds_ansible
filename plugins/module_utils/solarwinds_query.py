# -*- coding: utf-8 -*-
#
# Copyright: (c) 2022, Ashley Hooper <ashleyghooper@gmail.com>
#
# GNU General Public License v3.0+
# (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

import hashlib
import json
import traceback
from datetime import datetime

from ansible_collections.anophelesgreyhoe.solarwinds.plugins.module_utils.sql_query_builder import (
    SQLQueryBuilder,
)

# Utility functions


def table_alias(table_name):
    """
    Return the alias for a table.
    """
    return "_".join([u for u in table_name if u.isupper()]).lower()


def nested_column_prefix(table_name):
    """
    Return a unique prefix derived from the table name for use as a prefix
    for nested columns.
    """
    return "{0}{1}_".format(
        table_name[0], hashlib.sha1(table_name.encode("utf-8")).hexdigest()[0:4]
    )


def dict_key_is_truthy(dictionary, key, type):
    """
    Check key exists in a dictionary, is of the specified type, and has a truthy value.
    """
    return key in dictionary and isinstance(dictionary[key], type) and dictionary[key]


class SolarWindsQuery(object):
    """
    Class for facilitating dynamic SolarWinds Information Service queries.
    This is intended to enable automatic generation of a query according to
    the provided arguments, along with nesting of subordinate entities.
    """

    def __init__(self, module, solarwinds_client):
        self._module = module
        self._client = solarwinds_client

    def query(
        self,
        in_base_table,
        in_nested_entities,
        in_filters,
    ):
        queries = []
        base_table = in_base_table
        if in_nested_entities is None:
            in_nested_entities = {}
        output_format = self.output_format(in_base_table, in_nested_entities)
        base_table_name = base_table["name"]
        projected_columns = self.projected_columns(base_table_name, output_format)
        if not projected_columns:
            # Minimal column projection to ensure a valid SQL query
            projected_columns = [("no_output_columns", "1")]
        query = (
            SQLQueryBuilder()
            .SELECT(*projected_columns)
            .FROM("{0} AS {1}".format(base_table_name, table_alias(base_table_name)))
        )

        filters = []
        if in_filters:
            for filter in in_filters:
                filter_sql = []
                if dict_key_is_truthy(filter, "include", dict):
                    filter_sql.append(
                        self.where_clause(base_table_name, filter["include"])
                    )
                if dict_key_is_truthy(filter, "exclude", dict):
                    filter_sql.append(
                        "AND NOT {0}".format(
                            self.where_clause(base_table_name, filter["exclude"])
                        )
                    )
                filters.append("({0})".format(" ".join(filter_sql)))
            query.WHERE(" OR ".join(filters))

        try:
            queries.append(str(query))
            query_res = self._client.query(str(query))
        except Exception as ex:
            self._module.fail_json(
                msg=(
                    "Query for base table '{0}' failed. Queries: {1}. Exception: {2}".format(
                        base_table_name, str(queries), str(ex)
                    )
                )
            )

        data = []
        if dict_key_is_truthy(query_res, "results", list):
            if not dict_key_is_truthy(output_format, "nested", dict):
                # No grouping/nesting required
                data = query_res["results"]
            else:
                indexed = {}
                last_hashed = None
                for row in query_res["results"]:
                    unique = {
                        k: v
                        for k, v in row.items()
                        if k
                        in (
                            output_format["columns"]
                            if dict_key_is_truthy(output_format, "columns", list)
                            else []
                        )
                    }
                    hashed = hash(json.dumps(unique, sort_keys=True, default=str))
                    if last_hashed is None or hashed != last_hashed:
                        unique.update(
                            {k: [] for k in output_format["nested"].keys()}
                            if output_format["nested"]
                            else {}
                        )
                        indexed.setdefault(hashed, unique)
                    if output_format["nested"]:
                        for child_table in output_format["nested"].keys():
                            column_prefix = nested_column_prefix(child_table)
                            nested = {
                                k.replace(column_prefix, ""): v
                                for k, v in row.items()
                                if k not in output_format["columns"]
                                and k.startswith(column_prefix)
                            }
                            # Only append the nested value if it isn't already
                            # present, to avoid duplicates.
                            if nested not in indexed[hashed][child_table]:
                                indexed[hashed][child_table].append(nested)
                    last_hashed = hashed

                data = list(indexed.values())

        output = {
            "data": data,
        }
        if self._module._verbosity > 0:
            output["queries"] = str(queries)
        return output

    def output_format(self, base_table, nested_entities):
        base_table_name = base_table["name"]
        format = {}
        lookup_source_property_names = [
            t
            for t in nested_entities
            if nested_entities[t] is None
            or (nested_entities[t] is not None and "columns" not in nested_entities[t])
        ]

        # Look up properties for the base table if no columns specified for it,
        # or if one or more nested entities have no columns specified.
        lookup_entity_names = [
            base_table_name
            if lookup_source_property_names
            or "columns" not in base_table
            or base_table["columns"] is None
            else []
        ]

        entity_properties = self.retrieve_entity_properties(
            lookup_entity_names, lookup_source_property_names
        )
        format["columns"] = [
            (c)
            for c in (
                base_table["columns"]
                if "columns" in base_table
                and base_table["columns"] is not None
                and isinstance(base_table["columns"], list)
                else entity_properties[base_table_name]
            )
            if "." not in c
        ]

        all_nested_entities = list(
            set(
                [
                    c.split(".")[0]
                    for c in (
                        base_table["columns"]
                        if "columns" in base_table and base_table["columns"] is not None
                        else []
                    )
                    if "." in c
                ]
                + list(nested_entities.keys())
            )
        )

        format["nested"] = {}
        for entity_name in [
            e
            for e in all_nested_entities
            if e in nested_entities
            and nested_entities[e] is None
            or (
                nested_entities[e] is not None
                and dict_key_is_truthy(nested_entities[e], "columns", list)
            )
        ]:
            format["nested"][entity_name] = {}
            if nested_entities[entity_name] is None or (
                nested_entities[entity_name] is not None
                and "columns" not in nested_entities[entity_name]
            ):
                format["nested"][entity_name]["columns"] = entity_properties[
                    entity_name
                ]
            else:
                nested_via_base_table = [
                    c.split(".")[1]
                    for c in (
                        base_table["columns"]
                        if "columns" in base_table and base_table["columns"] is not None
                        else []
                    )
                    if c.split(".")[0] == entity_name
                ]
                nested = []
                if dict_key_is_truthy(nested_entities[entity_name], "columns", list):
                    nested = nested_entities[entity_name]["columns"]
                format["nested"][entity_name]["columns"] = list(
                    set(nested_via_base_table + nested)
                )
        return format

    def retrieve_entity_properties(self, entity_names, source_property_names):
        entity_names_sql = ",".join("'{0}'".format(e) for e in entity_names)
        if source_property_names:
            by_source_property_names_sql = " ".join(
                [
                    "UNION ALL (",
                    "SELECT DISTINCT P.Entity.Antecedents.SourcePropertyName as Lookup,",
                    "EntityName, Name",
                    "FROM Metadata.Property AS P",
                    "WHERE EntityName IN (",
                    "SELECT TargetType FROM Metadata.Relationship",
                    "WHERE SourceType IN (",
                    entity_names_sql,
                    ")",
                    "AND SourcePropertyName IN (",
                    ",".join("'{0}'".format(e) for e in source_property_names),
                    "))",
                    "AND IsNavigable = false",
                    ")",
                ]
            )
        else:
            by_source_property_names_sql = ""
        query = " ".join(
            [
                "SELECT DISTINCT EntityName AS Lookup, EntityName, Name",
                "FROM Metadata.Property WHERE EntityName IN (",
                entity_names_sql,
                ") AND IsNavigable = false",
                by_source_property_names_sql,
            ]
        )
        query_res = self._client.query(query)
        properties = {
            r["Lookup"]: [
                p["Name"] for p in query_res["results"] if p["Lookup"] == r["Lookup"]
            ]
            for r in query_res["results"]
        }
        return properties

    def projected_columns(self, base_table_name, output_format):
        projected_columns = list(
            list(
                (
                    output_format["columns"]
                    if "columns" in output_format
                    and isinstance(output_format["columns"], list)
                    else []
                )
            )
            + [
                "{a}.{t}.{c} AS {p}{c}".format(
                    a=table_alias(base_table_name), t=t, c=c, p=nested_column_prefix(t)
                )
                for t in (
                    output_format["nested"]
                    if "nested" in output_format
                    and isinstance(output_format["nested"], dict)
                    else {}
                )
                for c in output_format["nested"][t]["columns"]
            ]
        )
        return projected_columns

    def column_filters(self, filter_content):
        filters = []
        if isinstance(filter_content, dict):
            modifiers = filter_content.keys()
            for key in modifiers:
                modifier_filters = None
                if key in ["max", "min"]:
                    modifier_filters = self.column_filters(filter_content[key])
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
                try:
                    datetime.fromisoformat(criterion)
                    filters.append(("=", "'{0}'".format(criterion)))
                except Exception:
                    if isinstance(criterion, str):
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
                            filters.append(("LIKE", "'{0}'".format(criterion)))
                    elif isinstance(criterion, bool):
                        filters.append(("=", criterion))
                    elif isinstance(criterion, int) or isinstance(criterion, float):
                        filters.append(("=", criterion))
                    else:
                        # Unhandled data type
                        return False
        return filters

    def where_clause(self, base_table, filters):
        if filters is not None:
            criteria_sql_parts = []
            for item in filters:
                column_filters = self.column_filters(filters[item])
                if not column_filters:
                    self._module.fail_json(
                        msg="Filter criteria '{0}' not valid for property '{1}'".format(
                            str(filters[item]), item
                        )
                    )
                criteria_sql_part_column = " ".join(
                    [
                        ("OR " if i > 0 else "")
                        + "{0}.{1} {2} {3}".format(
                            table_alias(base_table), item, c[0], c[1]
                        )
                        for i, c in enumerate(column_filters)
                    ]
                )
                criteria_sql_parts.append("({0})".format(criteria_sql_part_column))
            return " AND ".join(criteria_sql_parts)
