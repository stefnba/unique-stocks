import typing as t

import polars as pl
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from custom.providers.iceberg.hooks.types import (
    CatalogConnectionProps,
    CatalogType,
    FileIOConnectionProps,
    is_aws_file_io_connection,
    is_sql_catalog_connection,
)
from pyiceberg import expressions
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.table import StaticTable
from pyiceberg.table import Table as IcebergTable
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

ALWAYS_TRUE = AlwaysTrue()

filter_expressions = expressions


class IcebergHook(BaseHook):
    """
    Query a Iceberg table using PyIceberg.
    """

    type: CatalogType

    io_conn_id: str
    catalog_conn_id: str
    io_connection: FileIOConnectionProps
    catalog_connection: CatalogConnectionProps

    catalog_name: str
    table_namespace_name: str
    table_name: str

    catalog: Catalog
    table: IcebergTable

    def __init__(
        self,
        io_conn_id: str,
        catalog_name: str,
        table_name: str | tuple[str, str],
        type: CatalogType = "hive",
        catalog_conn_id: str = "iceberg_catalog_default",
    ):
        self.type = type
        self.catalog_name = catalog_name

        if isinstance(table_name, tuple):
            self.table_namespace_name, self.table_name = table_name
        elif isinstance(table_name, str) and "." in table_name:
            self.table_namespace_name, self.table_name = table_name.split(".")
        else:
            raise TypeError(
                f"Table name '{table_name}' must either be a tuple of (table_namespace, table_name) "
                "or a string separating table_namespace and table_name a by a dot ."
            )

        self.io_conn_id = io_conn_id
        self.catalog_conn_id = catalog_conn_id

        self._init_file_io_conn()
        self._init_catalog_conn()

        self._load()

    def _load(self):
        self.load_catalog(self.catalog_name)
        self.load_table((self.table_namespace_name, self.table_name))

    def _init_file_io_conn(self):
        io_conn = self.get_connection(self.io_conn_id)

        if str(io_conn.conn_type) == "aws":
            self._set_aws_io_connection(io_conn)
        else:
            raise AirflowException(f"Connection type '{io_conn.conn_type}' is not supported for IO.")

    def _init_catalog_conn(self):
        catalog_conn = self.get_connection(self.catalog_conn_id)

        if self.type == "glue":
            self._set_glue_catalog_conn_props(catalog_conn)
        elif self.type == "sql":
            self.set_sql_catalog_connection(catalog_conn)
        elif self.type == "hive":
            self.set_hive_catalog_connection(catalog_conn)
        else:
            raise AirflowException(f"Connection type '{catalog_conn.conn_type}' is not supported for catalog.")

    def set_hive_catalog_connection(self, connection: Connection):
        """
        Specify Hive connection required for PyIceberg catalog if provided catalog connection type is `hive`
        and assign them to `catalog_connection` property.

        Args:
            connection (Connection): Catalog connection for specific arg `catalog_conn_id`.

        Raises:
            AirflowException: if `conn_type` is not `hive_metastore`.
            AirflowException: if `host` or `port` is not specified.
        """

        if not str(connection.conn_type) == "hive_metastore":
            raise AirflowException(f"Connection type {connection.conn_type} is not supported.")

        host = connection.host
        port = connection.port

        if not host:
            raise AirflowException(f"Host is not set in extra for connection '{self.catalog_conn_id}'.")

        if not port:
            raise AirflowException(f"Port is not set in extra for connection '{self.catalog_conn_id}'.")

        if "thrift://" in host:
            host = host.replace("thrift://", "")

        self.catalog_connection = {
            "uri": f"thrift://{host}:{port}",
            "type": "hive",
        }

    def set_sql_catalog_connection(self, connection: Connection):
        """
        Specify SQL connection required for PyIceberg catalog if provided catalog connection type is `sql`
        and assign them to `catalog_connection` property.

        Args:
            connection (Connection): Catalog connection for specific arg `catalog_conn_id`.

        Raises:
            AirflowException: if `uri` is not set in `extra` property of the connection.
        """

        if not str(connection.conn_type) == "postgres":
            raise AirflowException(f"Connection type {connection.conn_type} is not supported.")

        host = connection.host
        port = connection.port
        user = connection.login
        password = connection.password
        db = connection.schema

        self.catalog_connection = {"uri": f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}", "type": "sql"}

    def _set_aws_io_connection(self, connection: Connection):
        """
        Specify AWS credentials required for PyIceberg IO and assign them to `connection` property.

        Args:
            connection (Connection): IO connection for specific arg `io_conn_id`.

        Raises:
            AirflowException: if `region_name` is not set in `extra` property of the connection.
        """

        region = connection.extra_dejson.get("region_name")

        if not region:
            raise AirflowException(f"Missing property 'region_name' in extra for connection '{self.io_conn_id}'.")

        if not connection.login or not connection.password:
            raise AirflowException(f"Missing AWS credentials for connection '{self.io_conn_id}'.")

        self.io_connection = {
            "s3.access-key-id": connection.login,
            "s3.secret-access-key": connection.password,
            "s3.region": region,
        }

    def _set_glue_catalog_conn_props(self, connection: Connection):
        if self.catalog_conn_id == self.io_conn_id:
            # use io connection credentials for catalog
            if is_aws_file_io_connection(self.io_connection):
                self.catalog_connection = {
                    "aws_access_key_id": self.io_connection["s3.access-key-id"],
                    "aws_secret_access_key": self.io_connection["s3.secret-access-key"],
                    "region_name": self.io_connection["s3.region"],
                    "type": "glue",
                }
        else:
            raise AirflowException("Glue catalog connection must be set to the same connection as file IO.")

    def load_catalog(self, catalog_name: str) -> Catalog:
        """
        Load the catalog based on the properties.
        """

        if not self.catalog_connection:
            raise AirflowException("Catalog connection not initialized.")

        if not self.io_connection:
            raise AirflowException("IO connection not initialized.")

        props = t.cast(
            dict[str, str],
            {
                **self.catalog_connection,
                **self.io_connection,
            },
        )

        self.catalog = load_catalog(catalog_name, **props)

        return self.catalog

    def load_table(self, table_name: str | tuple[str, str]) -> IcebergTable:
        """
        Load the table's metadata and returns the table instance.
        """

        if not self.catalog:
            raise AirflowException("Catalog not initialized.")

        self.table = self.catalog.load_table(table_name)
        return self.table

    def to_arrow(
        self,
        row_filter: t.Union[str, BooleanExpression] = ALWAYS_TRUE,
        selected_fields: t.Tuple[str, ...] = ("*",),
    ):
        return self.table.scan(
            row_filter=row_filter,
            selected_fields=selected_fields,
        ).to_arrow()

    def to_pandas(
        self,
        row_filter: t.Union[str, BooleanExpression] = ALWAYS_TRUE,
        selected_fields: t.Tuple[str, ...] = ("*",),
    ):
        return self.table.scan(
            row_filter=row_filter,
            selected_fields=selected_fields,
        ).to_pandas()

    def to_duckdb(
        self,
        table_name: str,
        row_filter: t.Union[str, BooleanExpression] = ALWAYS_TRUE,
        selected_fields: t.Tuple[str, ...] = ("*",),
    ):
        return self.table.scan(
            row_filter=row_filter,
            selected_fields=selected_fields,
        ).to_duckdb(table_name)

    def to_polars(
        self,
        row_filter: t.Union[str, BooleanExpression] = ALWAYS_TRUE,
        selected_fields: t.Tuple[str, ...] = ("*",),
    ) -> pl.DataFrame:
        data = pl.from_arrow(
            self.to_arrow(
                row_filter=row_filter,
                selected_fields=selected_fields,
            )
        )

        if not isinstance(data, pl.DataFrame):
            raise TypeError("Data is not a polars DataFrame.")

        return data


class IcebergStaticTableHook(IcebergHook):
    """
    Load a table directly from a metadata file (i.e., without using a catalog).
    The static table is read-only.
    """

    def __init__(
        self,
        io_conn_id: str,
        catalog_name: str,
        table_name: str | tuple[str, str],
        type: CatalogType = "sql",
        catalog_conn_id: str = "iceberg_catalog_default",
    ):
        if type != "sql":
            raise AirflowException(
                f"IcebergStaticTableHook currently only support catalog type `sql`. Provided '{type}' is not supported."
            )

        super().__init__(io_conn_id, catalog_name, table_name, type, catalog_conn_id)

    def load_catalog(self, catalog_name: str) -> Catalog:
        return super().load_catalog(catalog_name)

    def _load(self):
        self.table = self.load_table((self.table_namespace_name, self.table_name))

    def load_table(
        self,
        table_name: str | tuple[str, str],
    ) -> IcebergTable:
        """
        Load Iceberg table from metadata json file.
        """

        if isinstance(table_name, tuple):
            table_namespace, table_name = table_name
        elif isinstance(table_name, str) and "." in table_name:
            table_namespace, table_name = table_name.split(".")
        else:
            raise TypeError(
                f"Table name '{table_name}' must either be a tuple of (table_namespace, table_name) "
                "or a string separating table_namespace and table_name a by a dot ."
            )

        uri = self.get_metadata_location(
            catalog_name=self.catalog_name, table_namespace=table_namespace, table_name=table_name
        )

        return StaticTable.from_metadata(uri, t.cast(dict, self.io_connection))

    def get_metadata_location(
        self,
        catalog_name: str,
        table_namespace: str,
        table_name: str,
    ) -> str:
        """
        Retrieve the metadata location from PostgreSQL catalog.

        Returns:
            str: Location to the metadata json file.
        """

        if not is_sql_catalog_connection(self.catalog_connection):
            raise TypeError("Catalog connection is not valid for catalog type 'sql'.")

        engine = create_engine(self.catalog_connection.get("uri"))

        with Session(engine, future=True) as sess:
            result = sess.execute(
                "SELECT * FROM iceberg_tables WHERE table_name = :tn AND table_namespace = :ts AND catalog_name = :cn",
                {
                    "cn": catalog_name,
                    "ts": table_namespace,
                    "tn": table_name,
                },
            )
            result = result.mappings().first().get("metadata_location")

            if not isinstance(result, str):
                raise TypeError(f"Result '{result}' is not a metadata URI.")

            return result
