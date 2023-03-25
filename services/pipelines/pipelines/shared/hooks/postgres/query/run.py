from typing import Optional

from psycopg.abc import Params
from shared.hooks.postgres.query.base import QueryBase
from shared.hooks.postgres.types import QueryInput


class RunQuery(QueryBase):
    def run(self, query: QueryInput, params: Optional[Params] = None):
        """
        Executes any query to the database.

        Args:
            query (Query): _description_
            params (Optional[Params], optional): _description_. Defaults to None.

        Returns:
            PgRecord:
        """
        return self._execute(query=query, params=params)
