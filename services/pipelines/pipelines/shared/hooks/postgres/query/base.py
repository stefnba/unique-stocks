from typing import Optional

import psycopg
from psycopg.abc import Params
from psycopg.rows import dict_row
from psycopg.sql import SQL, Composable, Composed, Identifier
from shared.hooks.postgres.query.record import PgRecord
from shared.hooks.postgres.types import QueryInput, ReturningParams
from shared.utils.sql.file import QueryFile

# from pydantic import BaseModel


class QueryBase:
    _conn_uri: str

    def _execute(self, query: QueryInput, params: Optional[Params] = None) -> PgRecord:
        """
        Executes a query to the database.
        Attention: Connection must be closed manually with .close()

        Args:
            query (Query): The query to execute.
            params (Optional[Params], optional): Parameters provided for query.
                Defaults to None.

        Returns:
            PgRecord: _description_
        """

        conn: psycopg.Connection | None = None

        query_as_string = self._query_as_string(query)

        try:
            with psycopg.connect(self._conn_uri, row_factory=dict_row) as conn:
                cur = conn.cursor()
                cur.execute(query=self._init_query(query), params=params)
                return PgRecord(cur)

        except psycopg.errors.UniqueViolation as error:
            print("vioatl", error.sqlstate, error.pgresult, query_as_string)
            raise

        finally:
            if conn:
                conn.close()

    def _init_query(self, query: QueryInput) -> Composed:
        """
        Translates type `Query` into `Composed` type.

        Args:
            query (Query): Query provided to method.

        Returns:
            Composed:
        """
        _query = SQL("")
        if isinstance(query, SQL):
            _query = query
        if isinstance(query, str):
            _query = SQL(query)
        if isinstance(query, QueryFile):
            _query = query.sql
        return Composed([_query])

    def _query_as_string(self, query: QueryInput) -> str:
        conn = psycopg.connect(self._conn_uri)
        _query = ""

        if isinstance(query, QueryFile):
            _query = query.sql.as_string(conn)
        if isinstance(query, Composable):
            _query = query.as_string(conn)
        elif isinstance(query, SQL):
            _query = query.as_string(conn)
        elif isinstance(query, str):
            _query = query

        return _query


class UpdateAddBase:
    def _concatenate_returning_query(self, returning: ReturningParams) -> Composed:
        return_clase = SQL(" RETURNING ")
        if isinstance(returning, str):
            if returning == "*":
                return Composed([return_clase, SQL("*")])

            return Composed([return_clase, SQL("{}").format(returning)])

        if isinstance(returning, list):
            return Composed([return_clase, SQL(", ").join(map(Identifier, returning))])

        return Composed("")
