import pathlib
from io import BytesIO

import duckdb
import polars as pl
from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
from custom.providers.azure.hooks.handlers.base import AzureDatasetHandler
from custom.providers.azure.hooks.types import Dataset
from pyarrow import dataset as ds


class AzureDatasetWriteArrowHandler(AzureDatasetHandler):
    """Write a dataset to Azure Data Lake Storage using `pyarrow.Dataset.write_dataset()`."""

    def write(
        self,
        dataset: Dataset,
        existing_data_behavior="error",
        basename_template=None,
        partitioning_flavor="hive",
        partitioning=None,
        format="parquet",
        max_rows_per_file=10 * 1024 * 1024,
        max_rows_per_group=256 * 1024,
        min_rows_per_group=64 * 1024,
        **kwargs,
    ):
        if pathlib.Path(self.path.uri).suffix:
            raise TypeError("Path must be a valid directory path, not a file path.")

        if isinstance(dataset, duckdb.DuckDBPyRelation):
            dataset = dataset.record_batch(max_rows_per_group)

        ds.write_dataset(
            data=dataset,
            base_dir=self.path.uri,
            filesystem=self.filesystem,
            format=format or self.format,
            existing_data_behavior=existing_data_behavior,
            partitioning_flavor=partitioning_flavor,
            basename_template=basename_template,
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
            min_rows_per_group=min_rows_per_group,
            partitioning=partitioning,
            **kwargs,
        )

        return self.path


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


class AzureWriteHandlers:
    Upload = AzureDatasetWriteUploadHandler
    Arrow = AzureDatasetWriteArrowHandler
