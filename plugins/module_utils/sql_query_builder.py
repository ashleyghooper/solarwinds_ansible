# -*- coding: utf-8 -*-
#
# Copyright: (c) 2022, Ashley Hooper <ashleyghooper@gmail.com>
#
# GNU General Public License v3.0+
# (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type


import functools
import textwrap
from collections import defaultdict
from typing import NamedTuple


class SQLQueryBuilder(object):
    """
    Generic SQL query builder for Python, heavily based on
    https://death.andgravity.com/query-builder-how with minor tweaks to suit
    the SolarWinds Information Service SQL dialect (SWQL).
    """

    keywords = [
        "SELECT",
        "FROM",
        "WHERE",
        "GROUP BY",
        "HAVING",
        "ORDER BY",
    ]

    separators = dict(WHERE="AND", HAVING="AND")
    default_separator = ","

    formats = (
        defaultdict(lambda: "{value}"),
        defaultdict(lambda: "{value} AS {alias}", WITH="{alias} AS {value}"),
    )

    subquery_keywords = {"WITH"}
    fake_keywords = dict(JOIN="FROM")
    flag_keywords = dict(SELECT={"DISTINCT", "ALL"})

    def __init__(self, data=None, separators=None):
        self.data = {}
        if data is None:
            data = dict.fromkeys(self.keywords, ())
        for keyword, args in data.items():
            self.data[keyword] = _FlagList()
            self.add(keyword, *args)

        if separators is not None:
            self.separators = separators

    def add(self, keyword, *args):
        keyword, fake_keyword = self._resolve_fakes(keyword)
        keyword, flag = self._resolve_flags(keyword)
        target = self.data[keyword]

        if flag:
            if target.flag:
                raise ValueError("{0} already has flag: {1!r}".format(keyword, flag))
            target.flag = flag

        kwargs = {}
        if fake_keyword:
            kwargs.update(keyword=fake_keyword)
        if keyword in self.subquery_keywords:
            kwargs.update(is_subquery=True)

        for arg in args:
            target.append(_Thing.from_arg(arg, **kwargs))

        return self

    def _resolve_fakes(self, keyword):
        for part, real in self.fake_keywords.items():
            if part in keyword:
                return real, keyword
        return keyword, ""

    def _resolve_flags(self, keyword):
        prefix, _, flag = keyword.partition(" ")  # pylint: disable=disallowed-name
        if prefix in self.flag_keywords:
            if flag and flag not in self.flag_keywords[prefix]:
                raise ValueError("invalid flag for {0}: {1!r}".format(prefix, flag))
            return prefix, flag
        return keyword, ""

    def __getattr__(self, name):
        if not name.isupper():
            return getattr(super(), name)
        return functools.partial(self.add, name.replace("_", " "))

    def __str__(self):
        return " ".join(self._parts())

    def _parts(self):
        for keyword, things in self.data.items():
            if not things:
                continue

            if things.flag:
                yield "{0} {1}".format(keyword, things.flag)
            else:
                yield keyword

            grouped = [], []
            for thing in things:
                grouped[bool(thing.keyword)].append(thing)
            for group in grouped:
                yield from self._parts_keyword(keyword, group)

    def _parts_keyword(self, keyword, things):
        for i, thing in enumerate(things, 1):
            last = i == len(things)

            if thing.keyword:
                yield thing.keyword

            format = self.formats[bool(thing.alias)][keyword]
            value = thing.value

            content = format.format(value=value, alias=thing.alias)

            if not last and not thing.keyword:
                try:
                    separator = self.separators[keyword]
                    yield "{0} {1}".format(content, separator)
                except KeyError:
                    separator = self.default_separator
                    yield "{0}{1}".format(content, separator)
            else:
                yield content


class _Thing(NamedTuple):
    value: str
    alias: str = ""
    keyword: str = ""
    is_subquery: bool = False

    @classmethod
    def from_arg(cls, arg, **kwargs):
        if isinstance(arg, str):
            alias, value = "", arg
        elif len(arg) == 2:
            alias, value = arg
        else:
            raise ValueError("invalid arg: {0!r}".format(arg))
        return cls(_clean_up(value), _clean_up(alias), **kwargs)


class _FlagList(list):
    flag: str = ""


def _clean_up(thing: str) -> str:
    return textwrap.dedent(thing.rstrip()).strip()
