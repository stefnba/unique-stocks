import pyarrow.dataset as ds
import polars as pl
import duckdb
from utils.filesystem.path import TempFilePath
from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
from custom.providers.azure.hooks.handlers.base import AzureDatasetReadBaseHandler, DatasetReadBaseHandler


class AzureDatasetDuckDbHandler(AzureDatasetReadBaseHandler):
    """Loads a dataset with duckdb and adlfs.AzureBlobFileSystem as filesystem."""

    def read(self):
        prefix = "abfs://"

        duckdb.register_filesystem(filesystem=self.filesystem)

        if self.format == "parquet":
            return duckdb.read_parquet(
                self._find_paths(self.path, container=self.container, prefix=prefix, use_adlfs_for_glob=False)
            )

        full_path = f"{prefix}{self.container}/{self.path}"

        if self.format == "json":
            return duckdb.read_json(full_path)

        if self.format == "csv":
            return duckdb.read_csv(full_path)

        raise ValueError("File format not supported.")


# class AzureDatasetArrowHandler(AzureDatasetReadBaseHandler):
#     """Loads a dataset with `pyarrow.dataset` and `adlfs.AzureBlobFileSystem` as filesystem."""

#     def read(self) -> pl.LazyFrame | duckdb.DuckDBPyRelation:
#         # pyarrow.dataset doesn't allow for glob pattern *, so use walk_adls_glob to achieve it
#         path = self._find_paths(path=self.path, container=self.container)

#         filesystem = self.filesystem
#         return duckdb.from_arrow(ds.dataset(path, filesystem=filesystem, format=format))


class AzureDatasetArrowHandler(AzureDatasetReadBaseHandler):
    """Reads a dataset with `pyarrow.dataset` and `adlfs.AzureBlobFileSystem` as filesystem."""

    def read(self, schema=None, **kwargs) -> ds.FileSystemDataset:
        # pyarrow.dataset doesn't allow for glob pattern *, so use walk_adls_glob to achieve it
        path = self._find_paths(path=self.path, container=self.container)

        print(1111, path, self.format, self.filesystem)

        d = ds.dataset(source=path, filesystem=self.filesystem, format=self.format, schema=schema)
        print("Ddd", d)
        return d


class LocalDatasetArrowHandler(DatasetReadBaseHandler):
    """Reads a dataset from local filesystem with `pyarrow.dataset`."""

    def read(self, schema=None, **kwargs) -> ds.FileSystemDataset:
        return ds.dataset(source=self.path, format=self.format, schema=schema)


class AzureDatasetStreamHandler(AzureDatasetReadBaseHandler):
    """
    Download a dataset (must be a single file) with `AzureDataLakeStorageHook` to a temporary local directory and scan
    with `polars`.
    """

    def read(self, **kwargs) -> pl.LazyFrame:
        path = self._find_paths(path=self.path, container=self.container)

        if isinstance(path, list):
            raise Exception("Multiple paths are not suported.")

        file_path = TempFilePath.create(file_format=self.format)

        # download
        AzureDataLakeStorageHook(conn_id=self.conn_id).download(container=self.container, blob_path=path)

        if self.format == "parquet":
            return pl.scan_parquet(file_path)
        if self.format == "csv":
            return pl.scan_csv(file_path)

        raise ValueError("File format not supported.")


class AzureDatasetReadHandler(AzureDatasetReadBaseHandler):
    """
    Read a dataset (must be a single file) with `AzureDataLakeStorageHook` as bytes directly into
    with `polars`.
    """

    def read(self, **kwargs) -> pl.LazyFrame:
        path = self.path

        if isinstance(path, list):
            raise Exception("Multiple paths are not suported.")

        content = AzureDataLakeStorageHook(conn_id=self.conn_id).read_blob(container=self.container, blob_path=path)

        return pl.read_json(content).lazy()


class LocalDatasetReadHandler(DatasetReadBaseHandler):
    """
    Lazy scan of a local dataset with `polars`.
    Works only for `parquet` and `csv` files.
    """

    no_container = True

    def read(self, **kwargs) -> pl.LazyFrame:
        path = self.path

        if isinstance(path, list):
            raise Exception("Multiple paths are not suported.")

        if self.format == "parquet":
            return pl.scan_parquet(path)
        if self.format == "csv":
            return pl.scan_csv(path)

        raise ValueError("File format not supported.")
