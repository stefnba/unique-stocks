from typing import Optional

from psycopg.abc import Params
from shared.hooks.postgres.query.base import QueryBase
from shared.hooks.postgres.query.filter import Filter
from shared.hooks.postgres.types import FilterParams, QueryInput

# from psycopg.sql import SQL


class FindQuery(QueryBase, Filter):
    def find(
        self,
        query: QueryInput,
        params: Optional[Params] = None,
        filters: Optional[FilterParams] = None,
    ):
        """
        Simplifies building of SELECT query.

        Args:
            query (QueryInput): SQL query.
            params (Optional[Params], optional): Parameters provided to SQL query. Defaults to None.
            filters (Optional[FilterParams], optional): Additional filter for WHERE clause of SELECT query.
            Defaults to None.
        """
        _query = self._init_query(query)

        if filters:
            _query += self._concatenate_where_query(filters)

        return self._execute(query=_query, params=params)
