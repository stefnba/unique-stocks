import psycopg

from services.hooks.postgres.query.query import PgQuery
from services.hooks.postgres.types import ConnectionInfo, ConnectionModel


class PgClient(PgQuery):
    def __init__(self, connection: ConnectionInfo) -> None:
        """_summary_

        Args:
            connection (ConnectionInfo): The connection string (a postgresql:// url)
            to specify where and how to connect.
        """
        self._create_connection(connection)

    def _create_connection(self, connection: ConnectionInfo):
        self._get_connection_uri(connection)

        # test
        if True:
            self._test_connection()

    def _get_connection_uri(self, connection: ConnectionInfo) -> str:
        """
        Creates a connection string from connection object or connection model

        Args:
            connection (ConnectionInfo): either connection string, object or model

        Returns:
            str: The connection string (a postgresql:// url)
                to specify where and how to connect.
        """
        if isinstance(connection, str):
            self._conn_uri = connection
        else:
            if isinstance(connection, ConnectionModel):
                conn = connection
            else:
                conn = ConnectionModel.parse_obj(connection)

            self._conn_uri = f"postgresql://{conn.user}:{conn.password}@{conn.host}:\
                {conn.port}/{conn.db_name}"

        return self._conn_uri

    def _test_connection(self):
        """
        Tests whether the provided connection details are valid.
        """
        with psycopg.connect(self._conn_uri) as conn:
            conn.execute("SELECT 1")
            print("✅ Connection successful")
