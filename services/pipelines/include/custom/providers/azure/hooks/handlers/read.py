from adlfs import AzureBlobFileSystem
import pyarrow.dataset as ds
from shared.types import DataLakeDataFileTypes

import polars as pl
from typing import Optional, cast
import duckdb
from utils.filesystem.path import TempFilePath
from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook


class ContainerNotSpecifiedError(Exception):
    """"""


def concatenate_path_container(path: str, container: str, prefix: Optional[str] = None):
    """Joins container name and path of file within container."""

    joined = "/".join([container, path])

    if prefix and not joined.startswith(prefix):
        return prefix + joined

    return joined


class AzureDatasetReadBaseHandler:
    path: str | list[str]
    container: Optional[str]
    format: DataLakeDataFileTypes
    filesystem: AzureBlobFileSystem
    conn_id: str

    no_container = False

    def __init__(
        self,
        source_path: str | list[str],
        container: Optional[str] = None,
        *,
        format: DataLakeDataFileTypes,
        filesystem: AzureBlobFileSystem,
        conn_id: str,
    ):
        self.path = source_path
        self.container = container
        self.format = format
        self.filesystem = filesystem
        self.conn_id = conn_id

        if not container and not self.no_container:
            raise ContainerNotSpecifiedError()

    def read(self, **kwargs) -> pl.LazyFrame | duckdb.DuckDBPyRelation | ds.FileSystemDataset:
        """"""
        raise NotImplementedError

    def _check_paths_adlfs(self, path: str) -> list[str] | str:
        """
        Use adlfs wrapper to get files if glob pattern found, otherwise return provided path as is..
        """
        if "*" in path or "**" in path or "?" in path or "[..]" in path:
            return cast(list[str], self.filesystem.glob(path))

        return path

    def _find_paths(self, path: str | list[str], container: str, prefix: Optional[str] = None, use_adlfs_for_glob=True):
        """
        Find all files for specified path and container.
        """

        if isinstance(path, list):
            all_paths: list[str] = []

            for p in path:
                if use_adlfs_for_glob:
                    matches = self._check_paths_adlfs(
                        concatenate_path_container(path=p, container=container, prefix=prefix)
                    )
                    if isinstance(matches, str):
                        all_paths.append(matches)
                    else:
                        print(len(matches))
                        all_paths.extend(matches)
                else:
                    all_paths.append(concatenate_path_container(path=p, container=container, prefix=prefix))

            return all_paths

        # add container
        if use_adlfs_for_glob:
            return self._check_paths_adlfs(concatenate_path_container(path=path, container=container, prefix=prefix))
        return concatenate_path_container(path=path, container=container, prefix=prefix)


class AzureDatasetDuckDbHandler(AzureDatasetReadBaseHandler):
    """Loads a dataset with duckdb and adlfs.AzureBlobFileSystem as filesystem."""

    def read(self):
        prefix = "abfs://"

        if not self.container:
            raise ContainerNotSpecifiedError()

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
    """Loads a dataset with `pyarrow.dataset` and `adlfs.AzureBlobFileSystem` as filesystem."""

    def read(self, schema=None, **kwargs) -> ds.FileSystemDataset:
        if not self.container:
            raise ContainerNotSpecifiedError()

        # pyarrow.dataset doesn't allow for glob pattern *, so use walk_adls_glob to achieve it
        path = self._find_paths(path=self.path, container=self.container)
        filesystem = self.filesystem
        return ds.dataset(source=path, filesystem=filesystem, format=self.format, schema=schema)


class LocalDatasetArrowHandler(AzureDatasetReadBaseHandler):
    """Loads a dataset from local filesystem with `pyarrow.dataset`."""

    def read(self, schema=None, **kwargs) -> ds.FileSystemDataset:
        return ds.dataset(source=self.path, format=self.format, schema=schema)


class AzureDatasetStreamHandler(AzureDatasetReadBaseHandler):
    """
    Download a dataset (must be a single file) with `AzureDataLakeStorageHook` to a temporary local directory and scan
    with `polars`.
    """

    def read(self, **kwargs) -> pl.LazyFrame:
        if not self.container:
            raise ContainerNotSpecifiedError()

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


class LocalDatasetReadHandler(AzureDatasetReadBaseHandler):
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
