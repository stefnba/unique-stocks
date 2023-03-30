# from psycopg import sql
import re
from typing import Any, cast

from psycopg._compat import LiteralString
from psycopg.sql import SQL, Composed, Identifier, Literal
from shared.hooks.postgres.types import FilterObject, FilterParams


class Filter:
    def _concatenate_where_query(self, filters: FilterParams) -> Composed:
        if isinstance(filters, str):
            # replace where claus if present in filter string
            filter_string = re.sub("where", "", filters, flags=re.IGNORECASE).lstrip()
            return Composed([SQL(" WHERE "), SQL(cast(LiteralString, filter_string))])
        if isinstance(filters, list):
            return self.__build_filter_list(filters)
        return Composed("")

    def __build_filter_list(self, filter_list: list[FilterObject]) -> Composed:
        if len(filter_list) == 0:
            return Composed("")

        filters = [self.__build_one_filter(f) for f in filter_list]  # can include None
        _filters = [f for f in filters if f is not None]

        if len(_filters) == 0:
            return Composed("")

        return Composed([SQL(" WHERE "), SQL(" AND ").join(_filters)])

    def __build_one_filter(self, filter_object: FilterObject) -> Composed | None:
        operator = filter_object["operator"] if "operator" in filter_object else "EQUAL"
        column = filter_object["column"]
        value = filter_object["value"]

        if not value:
            return None

        if operator == "EQUAL":
            return self.__build_standard_filter_clause(column=column, value=value, operator=SQL("="))
        if operator == "HIGHER":
            return self.__build_standard_filter_clause(column=column, value=value, operator=SQL(">"))
        if operator == "HIGHER_EQUAL":
            return self.__build_standard_filter_clause(column=column, value=value, operator=SQL(">="))
        if operator == "LOWER":
            return self.__build_standard_filter_clause(column=column, value=value, operator=SQL("<"))
        if operator == "LOWER_EQUAL":
            return self.__build_standard_filter_clause(column=column, value=value, operator=SQL("<="))
        if operator == "IS_NULL":
            return SQL("{column} IS NULL").format(column=Identifier(column))

        if operator == "IS_NOT_NULL":
            return SQL("{column} IS NOT NULL").format(column=Identifier(column))

        if operator == "IN":
            if not value:
                raise ValueError("Filter value not defined.")
            if not isinstance(value, list):
                raise ValueError("Filter value must be of type list for IN operator.")
            value_list = SQL(", ").join([Literal(v) for v in value])
            return SQL("{column} = ({value})").format(column=Identifier(column), value=value_list)

        return None

    def __build_standard_filter_clause(
        self, column: str, value: Any, operator: SQL = SQL("="), value_required=True
    ) -> Composed:
        if value_required and not value:
            raise ValueError("Filter value not defined.")

        return SQL("{column} {operator} {value}").format(
            column=Identifier(column),
            operator=operator,
            value=Literal(value),
        )
