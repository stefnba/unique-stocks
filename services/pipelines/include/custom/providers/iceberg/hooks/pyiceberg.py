import typing as t

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table

CatalogType = t.Literal["glue"]


class IcebergHook(BaseHook):
    """
    Query a Iceberg table using PyIceberg.
    """

    conn_id: str
    connection: dict
    type: CatalogType
    table_name: str
    catalog_name: str
    table: Table

    def __init__(
        self,
        conn_id: str,
        catalog_name: str,
        table_name: str,
        type: CatalogType = "glue",
    ):
        self.conn_id = conn_id
        self.catalog_name = catalog_name
        self.table_name = table_name
        self.type = type

        self.get_conn()

        self.table = self.load_catalog(self.catalog_name).load_table(self.table_name)

    def get_conn(self):
        """
        Retrieves connection from Airflow and initializes `self.connection` based on conn type.
        """

        conn = self.get_connection(self.conn_id)

        if conn.conn_type == "aws":
            region = conn.extra_dejson.get("region_name")

            if not region:
                raise AirflowException(f"Missing property 'region_name' in extra for connection '{self.conn_id}'.")

            self.connection = {
                "s3.access-key-id": conn.login,
                "s3.secret-access-key": conn.password,
                "s3.region": region,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            }

            # add extra properties for glue catalog
            if self.type == "glue":
                self.connection.update(
                    {
                        "aws_access_key_id": conn.login,
                        "aws_secret_access_key": conn.password,
                        "region_name": region,
                    }
                )

            return

        raise AirflowException(f"Connection type {conn.conn_type} is not supported.")

    def load_catalog(self, catalog_name: str) -> Catalog:
        """
        Load the catalog based on the properties.
        """

        if not self.connection:
            raise AirflowException("Connection not initialized.")

        self.catalog = load_catalog(
            catalog_name,
            **{
                **self.connection,
                "type": self.type,
            },
        )

        return self.catalog

    def load_table(self, table_name: str) -> Table:
        return self.load_catalog(self.catalog_name).load_table(table_name)

    def to_arrow(self):
        return self.table.scan().to_arrow()

    def to_pandas(self):
        return self.table.scan().to_pandas()

    def to_duckdb(self, table_name: str):
        return self.table.scan().to_duckdb(table_name)

    def to_polars(self):
        import polars as pl

        return pl.from_arrow(self.to_arrow())
