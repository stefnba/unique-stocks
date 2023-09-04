from airflow.hooks.base import BaseHook
import polars as pl
import psycopg
from psycopg.rows import dict_row
from custom.hooks.db.postgres.utils.record import PostgresRecord
from custom.hooks.db.postgres.utils.build_query import AddQueryBuilder, FindQueryBuilder, UpdateQueryBuilder
from typing import Optional
from custom.hooks.db.postgres.types import (
    PostgresTable,
    PostgresColumns,
    PostgresQuery,
    PostgresData,
    PostgresComposedQuery,
)


class PostgresBaseHook(BaseHook):
    conn_name_attr = "postgres_conn_id"
    default_conn_name = "postgres_default"
    conn_type = "postgres"

    postgres_conn_id: str
    conn_uri: str

    def __init__(self, postgres_conn_id: str = default_conn_name, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
        self.get_conn()

    def get_conn(self):
        """Create connection uri from Airflow connection."""
        conn = self.get_connection(self.postgres_conn_id)
        self.conn_uri = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        return self.conn_uri

    def test_connection(self):
        """Test connection."""
        try:
            with psycopg.connect(self.conn_uri) as conn:
                conn.execute("SELECT 1")
                return True
        except Exception as err:
            print(err)
            return False


class PostgresHook(PostgresBaseHook):
    def execute(self, query: PostgresComposedQuery, params=None):
        """Executes a SQL query."""

        with psycopg.connect(self.conn_uri, row_factory=dict_row) as conn:
            cur = conn.cursor()

            query_as_string = query.as_string(conn)

            cur.execute(query=query, params=params)
            return PostgresRecord(cur)

    def add(self, data: PostgresData, table: PostgresTable, columns: PostgresColumns):
        """Simplifies executing of INSERT query."""
        return self.execute(query=AddQueryBuilder(data=data, table=table, columns=columns).build())

    def update(self, data: PostgresData, table: PostgresTable, columns: PostgresColumns, filter):
        """Simplifies executing of UPDATE query. Only sinlge record possible."""
        return self.execute(query=UpdateQueryBuilder(data=data, table=table, columns=columns, filter=filter).build())

    def find(self, query: PostgresQuery, params: Optional[dict] = None, filter=None):
        """Simplifies executing of SELECT query."""
        return self.execute(query=FindQueryBuilder(query=query, filter=filter).build(), params=params)


class PostgresCopyHook(PostgresBaseHook):
    hook_name = "PostgresCopyHook"


class PostgresPolarsHook(PostgresBaseHook):
    hook_name = "PostgresPolarsHook"

    def dump_to_df(self, query: str) -> pl.DataFrame:
        """Run SQL query and return polars.DataFrame."""
        return pl.read_database(query=query, connection=self.conn_uri)

    def load_from_df(self, df: pl.DataFrame, table: str):
        """Add records from polars.DataFrame to database."""
        df.write_database(table_name=table, connection=self.conn_uri)
