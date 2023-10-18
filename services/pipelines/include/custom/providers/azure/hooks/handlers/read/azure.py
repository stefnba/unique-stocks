import pyarrow.dataset as ds
import polars as pl
import duckdb
from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
from custom.providers.azure.hooks.handlers.base import AzureDatasetHandler
from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook
from custom.providers.azure.hooks.types import Dataset
from utils.filesystem.path import Path, LocalPath
from deltalake import DeltaTable
import pathlib


class DownloadDeltaFiles(AzureDatasetHandler):
    """
    Use DeltaTable method `file_uris()` to get uri of all file paths and download them using
    `AzureDataLakeStorageBulkHook`.
    """

    def transform_file_uris(self, uri: str) -> str:
        return pathlib.Path(uri.replace(f"{self.path.protocol}:/", "")).relative_to("/curated").as_posix()

    def read(self, **kwargs) -> Dataset:
        self.path.set_protocol("abfs")

        files = DeltaTable(table_uri=self.path.uri, storage_options=self.storage_options).file_uris()

        hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")

        local_path = LocalPath.create_temp_dir_path().uri

        files = list(map(self.transform_file_uris, files))

        hook.download_blobs_from_list(container=self.path.root, destination_dir=local_path, blobs=files)

        return ds.dataset(source=local_path)


class AzureDatasetPolarsDeltaHandler(AzureDatasetHandler):
    """Scan Delta Table from ADLS using Polars and its `scan_delta()` method."""

    def read(self, **kwargs) -> Dataset:
        protocol = "azure"
        self.path.set_protocol(protocol)
        return pl.scan_delta(source=self.path.uri, storage_options=self.storage_options)


class AzureDatasetDeltaHandler(AzureDatasetHandler):
    """Scan Delta Table from ADLS using DeltaTable and convert it to `pyarrow_dataset`."""

    def read(self, **kwargs) -> Dataset:
        self.path.set_protocol("abfs")
        return DeltaTable(table_uri=self.path.uri, storage_options=self.storage_options).to_pyarrow_dataset()


class AzureDatasetDuckDbHandler(AzureDatasetHandler):
    """Loads a dataset with duckdb and adlfs.AzureBlobFileSystem as filesystem."""

    def read(self):
        duckdb.register_filesystem(filesystem=self.filesystem)

        protocol = "abfs"
        self.path.set_protocol(protocol)
        path = self.path.uri

        if self.format == "parquet":
            return duckdb.read_parquet(path)

        if self.format == "json":
            return duckdb.read_json(path)

        if self.format == "csv":
            return duckdb.read_csv(path)

        raise ValueError("File format not supported.")


class AzureDatasetArrowHandler(AzureDatasetHandler):
    """Reads a dataset with `pyarrow.dataset` and `adlfs.AzureBlobFileSystem` as filesystem."""

    def read(self, schema=None, **kwargs) -> ds.FileSystemDataset:
        d = ds.dataset(source=self.path.uri, filesystem=self.filesystem, format=self.format, schema=schema)
        return d


class AzureDatasetStreamHandler(AzureDatasetHandler):
    """
    Download a dataset (must be a single file) with `AzureDataLakeStorageHook` to a temporary local directory and scan
    with `polars`.
    """

    def read(self, **kwargs) -> pl.LazyFrame:
        file_path = Path.create_temp_file_path()

        # download
        AzureDataLakeStorageHook(conn_id=self.conn_id).download(**self.path.afls_path)

        if self.format == "parquet":
            return pl.scan_parquet(file_path.uri)
        if self.format == "csv":
            return pl.scan_csv(file_path.uri)

        raise ValueError("File format not supported.")


class AzureDatasetReadHandler(AzureDatasetHandler):
    """
    Read a dataset (must be a single file) with `AzureDataLakeStorageHook` as bytes directly into
    with `polars`.
    """

    def read(self, **kwargs) -> pl.LazyFrame:
        content = AzureDataLakeStorageHook(conn_id=self.conn_id).read_blob(**self.path.afls_path)

        if self.format == "json":
            return pl.read_json(content).lazy()
        if self.format == "csv":
            return pl.read_csv(content).lazy()
        if self.format == "parquet":
            return pl.read_parquet(content).lazy()

        raise Exception("File Format")


class AzureReadHandlers:
    DuckDb = AzureDatasetDuckDbHandler
    Arrow = AzureDatasetArrowHandler
    PolarsStream = AzureDatasetStreamHandler
    PolarsDelta = AzureDatasetPolarsDeltaHandler
    Delta = AzureDatasetPolarsDeltaHandler
    Read = AzureDatasetReadHandler
    DeltaDowonload = DownloadDeltaFiles
