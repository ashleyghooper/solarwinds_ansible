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


class SolarWindsQuery(object):
    """
    Class for facilitating dynamic SolarWinds Information Service queries.
    This is intended to enable automatic generation of a query according to
    the provided arguments, along with nesting of subordinate entities.
    """

    def table_alias(self, table_name):
        """
        Return the alias for a table.
        """
        return "_".join([u for u in table_name if u.isupper()]).lower()

    def nested_column_prefix(self, table_name):
        """
        Return a unique prefix derived from the table name for use as a prefix
        for nested columns.
        """
        return "{0}{1}_".format(
            table_name[0], hashlib.sha1(table_name.encode("utf-8")).hexdigest()[0:4]
        )

    def __init__(self, module, solarwinds_client):
        self._module = module
        self._client = solarwinds_client

    def query(
        self,
        in_base_table,
        in_output_columns=[],
        in_output_children={},
        in_includes={},
        in_excludes={},
    ):
        queries = []
        base_table = in_base_table
        output = {}
        output["columns"] = [
            (c)
            for c in (in_output_columns if isinstance(in_output_columns, list) else [])
            if not "." in c
        ]

        output["children"] = {
            c.split(".")[0]: {"columns": c.split(".")[1]}
            for c in (in_output_columns if isinstance(in_output_columns, list) else [])
            if "." in c
        }
        output["children"].update(
            {
                t: {
                    "columns": [
                        c
                        for c in (
                            in_output_children[t]
                            if isinstance(in_output_children, dict)
                            else {}
                        )
                    ]
                }
                for t in in_output_children
            }
        )

        projected_columns = [
            c
            for c in (
                output["columns"]
                if "columns" in output and isinstance(output["columns"], list)
                else []
            )
            + [
                "{0}.{1}.{2} AS {3}{4}".format(
                    self.table_alias(base_table), t, c, self.nested_column_prefix(t), c
                )
                for t in (
                    output["children"]
                    if "children" in output and isinstance(output["children"], dict)
                    else {}
                )
                for c in output["children"][t]["columns"]
            ]
        ]
        if not projected_columns:
            # Minimal column projection to ensure a valid SQL query
            projected_columns = [("no_output_columns", "1")]

        query = (
            SQLQueryBuilder()
            .SELECT(*projected_columns)
            .FROM("{0} AS {1}".format(base_table, self.table_alias(base_table)))
        )

        if in_includes:
            query.WHERE(self.where_clause(base_table, in_includes))
        if in_excludes:
            query.WHERE("NOT {0}".format(self.where_clause(base_table, in_excludes)))

        try:
            queries.append(str(query))
            query_res = self._client.query(str(query))
        except Exception as ex:
            self._module.fail_json(
                msg=(
                    "Query for base table '{0}' failed. Queries: {1}. Exception: {2}".format(
                        base_table, str(queries), str(ex)
                    )
                )
            )

        data = []
        if "results" in query_res and query_res["results"]:
            if not "children" in output or not output["children"]:
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
                            output["columns"]
                            if "columns" in output
                            and isinstance(output["columns"], list)
                            else []
                        )
                    }
                    hashed = hash(json.dumps(unique, sort_keys=True, default=str))
                    if last_hashed is None or hashed != last_hashed:
                        unique.update(
                            {k: [] for k in output["children"].keys()}
                            if output["children"]
                            else {}
                        )
                        indexed.setdefault(hashed, unique)
                    if output["children"]:
                        for child_table in output["children"].keys():
                            column_prefix = self.nested_column_prefix(child_table)
                            nested = {
                                k.replace(column_prefix, ""): v
                                for k, v in row.items()
                                if k not in output["columns"]
                                and k.startswith(column_prefix)
                            }
                            # Only append the nested value if it isn't already
                            # present, to avoid duplicates.
                            if nested not in indexed[hashed][child_table]:
                                indexed[hashed][child_table].append(nested)
                    last_hashed = hashed

                data = [v for v in indexed.values()]

        info = {
            "data": data,
            "count": len(data),
            "queries": str(queries),
        }
        return info

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
                            self.table_alias(base_table), item, c[0], c[1]
                        )
                        for i, c in enumerate(column_filters)
                    ]
                )
                criteria_sql_parts.append("({0})".format(criteria_sql_part_column))
            return " AND ".join(criteria_sql_parts)
