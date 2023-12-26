from typing import Literal

import polars as pl
import pyarrow.dataset as ds
from adlfs import AzureBlobFileSystem
from airflow.hooks.base import BaseHook
from custom.providers.azure.hooks.types import AzureDataLakeCredentials
from shared import schema
from shared.types import DataProducts, DataSources

MappingFields = Literal["is_virtual", "composite_code"]


class MappingDatasetHook(BaseHook):
    hook_name = "MappingDatasetHook"
    conn_name_attr = "conn_id"
    default_conn_name = "azure_data_lake"

    conn_id: str

    def __init__(self, conn_id: str = default_conn_name):
        self.conn_id = conn_id

        self.get_conn()

        self.credentials = self.get_conn()

        self.filesystem = AzureBlobFileSystem(
            account_name=self.credentials.account_name,
            client_id=self.credentials.client_id,
            client_secret=self.credentials.secret,
            tenant_id=self.credentials.tenant_id,
        )

    def get_conn(self):
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson or {}

        tenant = extra.get("tenant_id")

        if not tenant:
            raise ValueError('"tenant_id" is missing.')

        # use Active Directory auth
        account_name = conn.host
        client = conn.login
        secret = conn.password

        account_url = f"https://{conn.host}.blob.core.windows.net/"

        return AzureDataLakeCredentials(
            tenant_id=tenant,
            secret=secret,
            account_name=account_name,
            client_id=client,
            account_url=account_url,
        )

    def mapping(self, product: DataProducts, source: DataSources, field: MappingFields) -> pl.DataFrame:
        """Read general mapping dataset into `polars.DataFrame`."""
        PATH = "mapping"

        return self._get_mapping(path=PATH, product=product, source=source, field=field)

    def _get_mapping(self, path: str, product: DataProducts, source: DataSources, field: MappingFields) -> pl.DataFrame:
        """Read mapping dataset into `polars.DataFrame`."""

        container = "mapping"

        data = ds.dataset(
            source=f"{container}/{path}",
            filesystem=self.filesystem,
            format="csv",
            partitioning="hive",
            schema=schema.Mapping,
        )

        return (
            pl.scan_pyarrow_dataset(data)
            .filter(
                (pl.col("product") == product)
                & (pl.col("source") == source)
                & pl.col("is_active")
                & (pl.col("field") == field)
            )
            .collect()
        )
