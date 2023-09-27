import polars as pl
from pyarrow import dataset as ds
import duckdb
from io import BytesIO
from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
from custom.providers.azure.hooks.types import Dataset
from custom.providers.delta_table.hooks.delta_table import DeltaTableHook
from custom.providers.azure.hooks.handlers.base import AzureDatasetHandler


class AzureDatasetWriteArrowHandler(AzureDatasetHandler):
    """Write a dataset to Azure Data Lake Storage using `pyarrow.Dataset.write_dataset()`."""

    def write(
        self,
        dataset: Dataset,
        existing_data_behavior="error",
        basename_template=None,
        partitioning_flavor="hive",
        format="parquet",
        **kwargs,
    ):
        ds.write_dataset(
            data=dataset,
            base_dir=self.path.uri,
            filesystem=self.filesystem,
            format=format or self.format,
            existing_data_behavior=existing_data_behavior,
            partitioning_flavor=partitioning_flavor,
            basename_template=basename_template,
            **kwargs,
        )

        return self.path


class AzureDatasetWriteDeltaTableHandler(AzureDatasetHandler):
    """
    Upload and convert a dataset into a Delta Table with `DeltaTableHook`.
    The most efficient and memory optimal way is providing a `pyarrow.Dataset`. Since the
    `pyarrow.Dataset.to_batches()` method is used, a schema must be provided.
    """

    def write(self, dataset: Dataset, schema=None, **kwargs):
        hook = DeltaTableHook(self.conn_id)

        if isinstance(dataset, duckdb.DuckDBPyRelation):
            df = dataset.arrow()
            return hook.write(data=df, path=self.path)

        if isinstance(dataset, pl.LazyFrame):
            raise ValueError("AzureDatasetWriteDeltaTableHandler not yet supports writing a `pl.LazyFrame`.")

        if isinstance(dataset, ds.FileSystemDataset):
            hook.write(
                data=dataset,
                path=self.path,
                schema=schema or dataset.schema,
                **kwargs,
            )


class AzureDatasetWriteUploadHandler(AzureDatasetHandler):
    """
    Upload a dataset with `AzureDataLakeStorageHook`.
    """

    def write(self, dataset: Dataset, **kwargs):
        dataset = dataset

        df = pl.DataFrame()

        if isinstance(dataset, duckdb.DuckDBPyRelation):
            df = dataset.pl()

        if isinstance(dataset, pl.LazyFrame):
            df = dataset.collect()

        buffer = BytesIO()
        df.write_parquet(buffer)

        AzureDataLakeStorageHook(conn_id=self.conn_id).upload(
            **self.path.afls_path, data=buffer.getvalue(), overwrite=True
        )

        return self.path
