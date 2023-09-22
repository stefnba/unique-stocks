import polars as pl
from pyarrow import dataset as ds
import duckdb
from io import BytesIO
from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
from custom.providers.delta_table.hooks.delta_table import DeltaTableHook
from custom.providers.azure.hooks.handlers.base import DatasetWriteBaseHandler, AzureDatasetWriteBaseHandler


class AzureDatasetWriteDeltaTableHandler(AzureDatasetWriteBaseHandler):
    """
    Upload and convert a dataset into a Delta Table with `DeltaTableHook`.
    The most efficient and memory optimal way is providing a `pyarrow.Dataset`. Since the
    `pyarrow.Dataset.to_batches()` method is used, a schema must be provided.
    """

    def write(self, schema=None, **kwargs):
        dataset = self.dataset

        hook = DeltaTableHook(self.conn_id)

        if isinstance(dataset, duckdb.DuckDBPyRelation):
            df = dataset.arrow()
            return hook.write(data=df, destination_container=self.container, destination_path=self.path)

        if isinstance(dataset, pl.LazyFrame):
            raise ValueError("AzureDatasetWriteDeltaTableHandler not yet supports writing a `pl.LazyFrame`.")

        if isinstance(dataset, ds.FileSystemDataset):
            hook.write(
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

    def write(self):
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


class LocalDatasetWriteHandler(DatasetWriteBaseHandler):
    """
    Sink a `pl.LazyFrame` dataset to local filesystem.
    """

    def write(self):
        dataset = self.dataset

        if isinstance(dataset, pl.LazyFrame):
            dataset.sink_parquet(path=self.path)

        return self.path
