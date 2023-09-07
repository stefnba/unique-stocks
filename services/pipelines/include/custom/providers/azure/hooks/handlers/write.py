import polars as pl
from pyarrow import dataset as ds
import duckdb
from typing import TypeAlias
from adlfs import AzureBlobFileSystem
from io import BytesIO
from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
from custom.providers.delta_table.hooks.delta_table import DeltaTableHook

Dataset: TypeAlias = pl.LazyFrame | duckdb.DuckDBPyRelation | ds.FileSystemDataset


class AzureDatasetWriteBaseHandler:
    path: str
    dataset: Dataset

    def __init__(
        self,
        path: str,
        dataset: Dataset,
        container: str,
        filesystem: AzureBlobFileSystem,
        conn_id: str,
    ):
        self.path = path
        self.dataset = dataset
        self.container = container
        self.filesystem = filesystem
        self.conn_id = conn_id

    def sink(self, **kwargs) -> str:
        raise NotImplementedError()


class AzureDatasetWriteDeltaTableHandler(AzureDatasetWriteBaseHandler):
    """
    Upload and convert a dataset into a Delta Table with `DeltaTableHook`.
    The most efficient and memory optimal way is providing a `pyarrow.Dataset`. Since the
    `pyarrow.Dataset.to_batches()` method is used, a schema must be provided.
    """

    def sink(self, schema=None, **kwargs):
        dataset = self.dataset

        hook = DeltaTableHook(self.conn_id)

        if isinstance(dataset, duckdb.DuckDBPyRelation):
            df = dataset.arrow()
            return hook.write(data=df, destination_container=self.container, destination_path=self.path)

        if isinstance(dataset, pl.LazyFrame):
            raise ValueError("AzureDatasetWriteDeltaTableHandler not yet supports writing a `pl.LazyFrame`.")

        if isinstance(dataset, ds.FileSystemDataset):
            return hook.write(
                data=dataset.to_batches(),
                destination_container=self.container,
                destination_path=self.path,
                schema=schema or dataset.schema,
                **kwargs,
            )


class AzureDatasetWriteUploadHandler(AzureDatasetWriteBaseHandler):
    """
    Upload a dataset with `AzureDataLakeStorageHook`.
    """

    def sink(self):
        dataset = self.dataset

        df = pl.DataFrame()

        if isinstance(dataset, duckdb.DuckDBPyRelation):
            df = dataset.pl()

        if isinstance(dataset, pl.LazyFrame):
            df = dataset.collect()

        buffer = BytesIO()
        df.write_parquet(buffer)

        AzureDataLakeStorageHook(conn_id=self.conn_id).upload(
            data=buffer.getvalue(), container=self.container, blob_path=self.path, overwrite=True
        )

        return self.path
