from typing import Optional

from psycopg.abc import Params, Query

from services.hooks.postgres.query.base import QueryBase


class RunQuery(QueryBase):
    def run(self, query: Query, params: Optional[Params] = None):
        """
        Executes any query to the database.

        Args:
            query (Query): _description_
            params (Optional[Params], optional): _description_. Defaults to None.

        Returns:
            PgRecord:
        """
        return self._execute(query=query, params=params)
