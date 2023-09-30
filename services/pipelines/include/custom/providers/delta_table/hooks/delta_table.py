from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from airflow.hooks.base import BaseHook
from typing import Dict, Literal, List, cast
from utils.filesystem.path import Path, PathInput
from custom.providers.delta_table.hooks.types import DeltaTableStorageOptions
from pyarrow import dataset as ds


class DeltaTableHook(BaseHook):
    conn_id: str
    storage_options: Dict[str, str]

    protocol = "abfs"

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
        path: PathInput,
        version: int | None = None,
    ):
        path = Path.create(path)
        path.set_protocol(self.protocol)
        self.log.info(f"Reading Delta Table from '{path.uri}'.")
        return DeltaTable(table_uri=path.uri, storage_options=self.storage_options, version=version)

    def write(
        self,
        data: ds.FileSystemDataset,
        path: PathInput,
        schema=None,
        partition_by: List[str] | str | None = None,
        mode: Literal["error", "append", "overwrite", "ignore"] = "error",
        overwrite_schema: bool = False,
    ):
        path = Path.create(path)
        path.set_protocol(self.protocol)

        self.log.info(f"Writing Delta Table to '{path.uri}' with mode '{mode}'.")

        if not isinstance(data, ds.FileSystemDataset):
            raise TypeError("Dataset is not a valid `pyarrow.FileSystemDataset`.")

        write_deltalake(
            table_or_uri=path.uri,
            data=data.to_batches(),
            schema=schema or data.schema,
            partition_by=partition_by,
            mode=mode,
            overwrite_schema=overwrite_schema,
            storage_options=self.storage_options,
        )

        self.log.info(f"Finished writing Delta Table to '{path.uri}' with mode '{mode}'.")

        return path
