from typing import Optional

from psycopg.abc import Params, Query
from shared.hooks.postgres.query.base import QueryBase
from shared.hooks.postgres.query.filter import Filter
from shared.hooks.postgres.types import FilterParams

# from psycopg.sql import SQL


class FindQuery(QueryBase, Filter):
    def find(
        self,
        query: Query,
        params: Optional[Params] = None,
        filters: Optional[FilterParams] = None,
    ):
        """
        Simplifies building of SELECT query.

        Args:
            query (Query): _description_
            params (Optional[Params], optional): _description_. Defaults to None.
            filters (Optional[FilterParams], optional): _description_. Defaults to None.
        """
        _query = self._init_query(query)

        if filters:
            _query += self._concatenate_where_query(filters)

        return self._execute(query=_query, params=params)
