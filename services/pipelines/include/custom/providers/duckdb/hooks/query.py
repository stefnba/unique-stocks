import typing as t
from string import Template

import duckdb
from adlfs import AzureBlobFileSystem
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection


class DuckDBQueryHook(BaseHook):
    duck: duckdb.DuckDBPyConnection
    conn_id: t.Optional[str]

    def __init__(
        self,
        conn_id: t.Optional[str] = None,
    ):
        self.conn_id = conn_id

        self._init_db()
        self.get_conn()

    def get_conn(self) -> t.Any:
        if not self.conn_id:
            return

        conn = self.get_connection(self.conn_id)

        if conn.conn_type == "aws":
            return self.register_aws_conn(conn)

        if conn.conn_type == "adls":
            return self.register_adls_conn(conn)

        raise AirflowException(f"Connection type {conn.conn_type} not supported.")

    def query(self, query: str, params: t.Optional[t.Dict[str, str]] = None):
        """Run a duckDb query."""

        if not params:
            params = {}

        query = self._build_query(
            query,
            bindings={
                **params,
            },
        )

        return self.duck.sql(query)

    def _build_query(self, query: str, bindings: t.Dict[str, str]) -> str:
        """
        Build the query.
        """

        return Template(query).safe_substitute(bindings)

    def _init_db(self):
        self.duck = duckdb.connect(":memory:")

    def register_aws_conn(self, conn: Connection):
        """
        Load the AWS connection into the DuckDB instance to interact with files on S3.
        """

        extra = conn.extra_dejson

        region = extra.get("region_name")
        client_id = conn.login
        secret = conn.password

        if not region or not client_id or not secret:
            raise AirflowException("AWS connection must have region_name, login and password set.")

        # self.duck.execute("INSTALL httpfs")
        # self.duck.execute("LOAD httpfs")
        self.duck.execute(f"SET s3_region='{region}'")
        self.duck.execute(f"SET s3_access_key_id='{client_id}'")
        self.duck.execute(f"SET s3_secret_access_key='{secret}'")

    def register_adls_conn(self, conn: Connection):
        """
        Register the ADLS connection into the DuckDB instance to interact with files on ADLS.
        """

        extra = conn.extra_dejson

        tenant_id = extra.get("tenant_id")
        account_name = conn.host
        client_id = conn.login
        secret = conn.password

        if not account_name or not client_id or not secret or not tenant_id:
            raise AirflowException("ADLS connection must have login and password set.")

        self.duck.register_filesystem(
            AzureBlobFileSystem(
                account_name=account_name,
                client_id=client_id,
                client_secret=secret,
                tenant_id=tenant_id,
            )
        )
