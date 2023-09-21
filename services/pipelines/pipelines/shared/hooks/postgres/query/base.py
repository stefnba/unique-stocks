from typing import Optional

import psycopg
from psycopg.abc import Params
from psycopg.rows import dict_row
from psycopg.sql import SQL, Composable, Composed, Identifier
from shared.hooks.postgres.query.record import PgRecord
from shared.hooks.postgres.types import QueryInput, ReturningParams
from shared.utils.sql.file import QueryFile
from shared.loggers import logger, events as logger_events


class QueryBase:
    _conn_uri: str

    def _execute(
        self, query: QueryInput, params: Optional[Params] = None, table: Optional[str | tuple[str, str]] = None
    ) -> PgRecord:
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

        # cut long query to 500 characters
        if len(query_as_string) > 500:
            query_as_string = query_as_string[0:500]

        logger.db.info(event=logger_events.database.QueryExecution(query=query_as_string, table=table))

        try:
            with psycopg.connect(self._conn_uri, row_factory=dict_row) as conn:
                cur = conn.cursor()
                cur.execute(query=query if not isinstance(query, QueryFile) else query.sql, params=params)
                return PgRecord(cur, query=query_as_string, table=table)

        except psycopg.errors.UniqueViolation as error:
            print("vioalation", error.sqlstate, error.pgresult, query_as_string)
            raise

        except psycopg.errors.ForeignKeyViolation as error:
            print("vioalation", error.sqlstate, error.pgresult, query_as_string)
            raise

        except psycopg.errors.NotNullViolation as error:
            print("vioalation", error.sqlstate, error.pgresult, query_as_string)
            raise

        except Exception as error:
            logger.db.error(str(error), event=logger_events.database.Query(query=query_as_string))
            raise

        finally:
            if conn:
                conn.close()

    def _init_query(self, query: QueryInput) -> Composed:
        """
        Converts a query of type `Query` into a `Composed` type.
        """
        if isinstance(query, Composed):
            return query
        if isinstance(query, SQL):
            return Composed([query])
        if isinstance(query, str):
            return Composed([SQL(query)])
        if isinstance(query, QueryFile):
            return Composed([SQL(query.sql)])
        if isinstance(query, bytes):
            return Composed([query.decode()])

    def _query_as_string(self, query: QueryInput) -> str:
        """
        Converts final query from any type QueryInput into string.
        Useful for logging, etc.
        """
        conn = psycopg.connect(self._conn_uri)

        if isinstance(query, QueryFile):
            return str(query.sql)
        if isinstance(query, Composable) or isinstance(query, SQL):
            return query.as_string(conn)
        if isinstance(query, str):
            return query
        if isinstance(query, bytes):
            return query.decode()
