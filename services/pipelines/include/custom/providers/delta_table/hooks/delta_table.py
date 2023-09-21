from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from airflow.hooks.base import BaseHook
from custom.providers.azure.hooks.types import DatasetReadPath, DatasetWritePath
from typing import Optional, TypedDict, Dict, Literal, List, cast
from utils.filesystem.data_lake.base import DataLakePathBase


class DeltaTableStorageOptions(TypedDict):
    account_name: str
    client_id: str
    tenant_id: str
    client_secret: str


class DeltaTableHook(BaseHook):
    conn_id: str
    storage_options: Dict[str, str]

    prefix = "abfs://"

    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.storage_options = cast(Dict[str, str], self.get_conn())

    def get_conn(self) -> DeltaTableStorageOptions:
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson or {}

        tenant = extra.get("tenant_id")

        if not tenant:
            raise ValueError('"tenant_id" is missing.')

        account_name = conn.host
        client = conn.login
        secret = conn.password

        return {
            "account_name": account_name,
            "client_id": client,
            "tenant_id": tenant,
            "client_secret": secret,
        }

    def read(
        self,
        source_path: DatasetReadPath,
        source_container: Optional[str] = None,
        version: int | None = None,
    ):
        if isinstance(source_path, DataLakePathBase):
            source_container = source_path.container
            source_path = source_path.path

        table_uri = f"{self.prefix}{source_container}/{source_path}"

        return DeltaTable(table_uri=table_uri, storage_options=self.storage_options, version=version)

    def write(
        self,
        data,
        destination_path: DatasetWritePath,
        destination_container: Optional[str] = None,
        schema=None,
        partition_by: List[str] | str | None = None,
        mode: Literal["error", "append", "overwrite", "ignore"] = "error",
        overwrite_schema: bool = False,
    ):
        if isinstance(destination_path, DataLakePathBase):
            destination_container = destination_path.container
            destination_path = destination_path.path

        table_uri = f"{self.prefix}{destination_container}/{destination_path}"
        write_deltalake(
            table_or_uri=table_uri,
            data=data,
            schema=schema,
            partition_by=partition_by,
            mode=mode,
            overwrite_schema=overwrite_schema,
            storage_options=self.storage_options,
        )

        return table_uri
