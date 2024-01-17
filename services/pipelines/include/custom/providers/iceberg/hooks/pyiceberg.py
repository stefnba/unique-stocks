import typing as t

import polars as pl
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from pyiceberg import expressions
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import AlwaysTrue, BooleanExpression
from pyiceberg.table import Table

CatalogType = t.Literal["glue"]

ALWAYS_TRUE = AlwaysTrue()

filter_expressions = expressions


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
