from typing import Optional, overload

import psycopg
from shared.hooks.postgres.query.query import PgQuery
from shared.hooks.postgres.types import ConnectionModel, ConnectionObject
from shared.loggers import logger


class PgClient(PgQuery):
    @overload
    def __init__(self, conn: ConnectionObject) -> None:
        ...

    @overload
    def __init__(self, *, conn_uri: str) -> None:
        ...

    @overload
    def __init__(self, *, conn_id: str) -> None:
        ...

    def __init__(
        self,
        conn: Optional[ConnectionObject] = None,
        conn_uri: Optional[str] = None,
        conn_id: Optional[str] = None,
    ) -> None:
        """
        Create a PostgreSQL client.

        Args:
            conn (ConnectionObject, optional): A connection object.
            conn_uri (str, optional): A connection uri.
            conn_id (str, optional):  An Airflow connection id.
        """

        self._create_connection(conn=conn, conn_uri=conn_uri, conn_id=conn_id)

        # test
        if True:
            self._test_connection()

    def _create_connection(
        self,
        conn: Optional[ConnectionObject] = None,
        conn_uri: Optional[str] = None,
        conn_id: Optional[str] = None,
    ):
        """
        Specifies PostgresSQL connection uri from various connection options, e.g. ConnectionObject, uri, Airflow
        connection id.

        Args:
            conn (Optional[ConnectionObject], optional): _description_. A connection object.
            conn_uri (Optional[str], optional): _description_. The connection string (a postgresql:// uri).
            conn_id (Optional[str], optional): _description_. An Airflow connection id.

        Raises:
            Exception: In case not connection details provided.
        """
        if conn:
            self._conn_uri = self._create_connection_uri(conn)
            return
        elif conn_uri:
            self._conn_uri = conn_uri
            return
        elif conn_id:
            self._conn_uri = self._get_conn_uri_from_airflow_conn_id(conn_id)
            return

        raise Exception("No Connection details provided.")

    def _get_conn_uri_from_airflow_conn_id(self, connection_id: str) -> str:
        """
        Get a PostgreSQL connection uri from an Airflow connection id.

        Args:
            connection_id (str): Airflow connection id.

        Returns:
            str: The connection string (a postgresql:// uri)
                to specify where and how to connect.
        """
        try:
            from airflow.hooks.base import BaseHook
        except ImportError:
            raise Exception("Airflow not installed.")

        conn = BaseHook.get_connection(connection_id)
        return conn.get_uri()

    def _create_connection_uri(self, connection: ConnectionObject) -> str:
        """
        Creates a connection string from connection object.

        Args:
            connection (ConnectionObject): object specifying connection details such as host, user, db_name, ...

        Returns:
            str: The connection string (a postgresql:// uri)
                to specify where and how to connect.
        """

        conn = ConnectionModel.parse_obj(connection)

        return f"postgresql://{conn.user}:{conn.password}@{conn.host}:{conn.port}/{conn.db_name}"

    def _test_connection(self):
        """
        Tests whether the provided connection details are valid.
        """
        with psycopg.connect(self._conn_uri) as conn:
            conn.execute("SELECT 1")
            logger.db.info("✅ Connection successful")
