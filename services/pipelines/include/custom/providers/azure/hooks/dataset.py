from airflow.hooks.base import BaseHook
from adlfs import AzureBlobFileSystem
import pyarrow.dataset as ds
from shared.types import DataLakeDataFileTypes
from settings import config_settings
import polars as pl
from typing import Optional, Literal, overload, cast
import duckdb
from utils.filesystem.path import TempDirPath, TempFilePath
from utils.filesystem.data_lake.base import DataLakePathBase

from custom.providers.azure.hooks.types import (
    AzureDataLakeCredentials,
    DatasetType,
    DatasetReadPath,
    DatasetWritePath,
)

from custom.providers.azure.hooks.handlers.base import DatasetReadBaseHandler, DatasetWriteBaseHandler
from custom.providers.azure.hooks.handlers.read import AzureDatasetReadHandler
from custom.providers.azure.hooks.handlers.write import AzureDatasetWriteUploadHandler


class AzureDatasetHook(BaseHook):
    conn_id: str
    account_name: str
    container: Optional[str]
    filesystem: AzureBlobFileSystem
    credentials: AzureDataLakeCredentials

    def __init__(
        self, conn_id: str, account_name: Optional[str] = None, default_container: Optional[str] = None
    ) -> None:
        self.conn_id = conn_id

        self.credentials = self.get_conn()

        self.filesystem = AzureBlobFileSystem(
            account_name=account_name or self.credentials.account_name,
            client_id=self.credentials.client_id,
            client_secret=self.credentials.secret,
            tenant_id=self.credentials.tenant_id,
        )

        self.container = default_container

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

    def write(
        self,
        dataset: pl.LazyFrame | duckdb.DuckDBPyRelation | ds.FileSystemDataset,
        destination_path: DatasetWritePath,
        destination_container: Optional[str] = None,
        handler: type[DatasetWriteBaseHandler] = AzureDatasetWriteUploadHandler,
        **kwargs,
    ) -> str:
        """
        Save a dataset in Azure Data Lake Storage.
        The arg handler specifies the mehtod with which transfer is performend.

        Args:
            - dataset (pl.LazyFrame | duckdb.DuckDBPyRelation):
            The dataset to be saved.
            - destination_path (str):
            The path in the Data Lake where the file should be stored.
            - handler (SinkHandler, optional): _description_. Defaults to "Upload".

        Returns:
            str: Path where the file was saved. If dataset is partitioned, directory path is returned.
        """

        container = destination_container or self.container

        if isinstance(destination_path, DataLakePathBase):
            container = destination_path.container
            destination_path = destination_path.path

        _handler = handler(
            path=destination_path,
            dataset=dataset,
            conn_id=self.conn_id,
            container=container,
            filesystem=self.filesystem,
        )
        return _handler.write(**kwargs)

        return destination_path

        if isinstance(dataset, pl.LazyFrame):
            dataset.sink_parquet(TempFilePath.create())

        if isinstance(dataset, duckdb.DuckDBPyRelation):
            base_dir = TempDirPath.create()
            ds.write_dataset(dataset.fetch_arrow_reader(), base_dir=base_dir, format="parquet")
            return cast(str, base_dir)  # to correct false positive of mypy

        return destination_path

    @overload
    def read(
        self,
        source_path: DatasetReadPath,
        source_format: DataLakeDataFileTypes = ...,
        source_container: Optional[str] = ...,
        handler: type[DatasetReadBaseHandler] = ...,
        *,
        dataset_type: Literal["DuckDBRel", "DuckDBLocalScan"],
        **kwargs,
    ) -> duckdb.DuckDBPyRelation:
        ...

    @overload
    def read(
        self,
        source_path: DatasetReadPath,
        source_format: DataLakeDataFileTypes = ...,
        source_container: Optional[str] = ...,
        handler: type[DatasetReadBaseHandler] = ...,
        *,
        dataset_type: Literal["PolarsDataFrame"],
        **kwargs,
    ) -> pl.DataFrame:
        ...

    @overload
    def read(
        self,
        source_path: DatasetReadPath,
        source_format: DataLakeDataFileTypes = ...,
        source_container: Optional[str] = ...,
        handler: type[DatasetReadBaseHandler] = ...,
        *,
        dataset_type: Literal["PolarsLocalScan"],
        **kwargs,
    ) -> pl.LazyFrame:
        ...

    @overload
    def read(
        self,
        source_path: DatasetReadPath,
        source_format: DataLakeDataFileTypes = ...,
        source_container: Optional[str] = ...,
        handler: type[DatasetReadBaseHandler] = ...,
        *,
        dataset_type: Literal["PolarsLazyFrame"] = ...,
        **kwargs,
    ) -> pl.LazyFrame:
        ...

    @overload
    def read(
        self,
        source_path: DatasetReadPath,
        source_format: DataLakeDataFileTypes = ...,
        source_container: Optional[str] = ...,
        handler: type[DatasetReadBaseHandler] = ...,
        *,
        dataset_type: Literal["ArrowDataset"] = ...,
        **kwargs,
    ) -> ds.FileSystemDataset:
        ...

    def read(
        self,
        source_path: DatasetReadPath,
        source_format: DataLakeDataFileTypes = "parquet",
        source_container: Optional[str] = None,
        handler: type[DatasetReadBaseHandler] = AzureDatasetReadHandler,
        dataset_type: DatasetType = "DuckDBRel",
        **kwargs,
    ) -> duckdb.DuckDBPyRelation | pl.LazyFrame | pl.DataFrame | str | ds.FileSystemDataset:
        """
        Loads a dataset and return it.

        The method to load is specified by the `handler` arg and how the dataset is returned is specified by
        the `dataset_type` arg.

        Args:
            source_path (SourcePath): _description_
            handler (LoadHandler, optional): Method to load the dataset. Defaults to "ArrowDataset".
            source_format (DataLakeDataFileTypes, optional): _description_. Defaults to "parquet".
            container (Optional[str], optional): _description_. Defaults to None.

        Returns:
            Dataset either as duckdb.DuckDBPyRelation | pl.LazyFrame | pl.DataFrame | file path string depending on
            specified `dataset_type` arg.
        """

        container = source_container or self.container
        # path = source_path

        if isinstance(source_path, DataLakePathBase):
            container = source_path.container
            source_path = source_path.path

        # read dataset with specified handler
        dataset = handler(
            path=source_path,
            format=source_format,
            container=container,
            filesystem=self.filesystem,
            conn_id=self.conn_id,
        ).read(**kwargs)

        # return dataset depending on dataset_type
        if isinstance(dataset, duckdb.DuckDBPyRelation):
            if dataset_type == "DuckDBRel":
                return dataset

            if dataset_type == "PolarsDataFrame":
                return dataset.pl()

        if isinstance(dataset, pl.LazyFrame):
            if dataset_type == "PolarsDataFrame":
                return dataset.collect()

            if dataset_type == "PolarsLazyFrame":
                return dataset

        if isinstance(dataset, ds.FileSystemDataset):
            if dataset_type == "ArrowDataset":
                return dataset

        raise Exception("Handler not supported.")

        #     if dataset_type == "PolarsDataFrame":
        #         return rel.pl()

        #     if dataset_type == "PolarsLazyFrame":
        #         return rel.pl().lazy()

        #     return rel

        # if handler == "ArrowDataset":
        #     py_ds = self.dataset_load_handler_arrow(source_path=source_path, container=container, format=source_format)

        #     if dataset_type == "DuckDBRel":
        #         return duckdb.from_arrow(py_ds)
        #     if dataset_type == "PolarsDataFrame":
        #         return duckdb.from_arrow(py_ds).pl()
        #     if dataset_type == "PolarsLocalScan":
        #         dir_path = TempDirPath.create()
        #         ds.write_dataset(py_ds, base_dir=dir_path, format="parquet")
        #         return pl.scan_parquet(f"{dir_path}/*")
        #     if dataset_type == "DuckDBLocalScan":
        #         dir_path = TempDirPath.create()
        #         ds.write_dataset(py_ds, base_dir=dir_path, format="parquet")
        #         return duckdb.read_parquet(f"{dir_path}/*")
        #     if dataset_type == "PolarsLazyFrame":
        #         return pl.LazyFrame(duckdb.from_arrow(py_ds).arrow())

        # if handler == "Read":
        #     if isinstance(source_path, list):
        #         raise Exception("Multiple paths not allowed.")

        #     data = AzureDataLakeStorageHook(conn_id=self.conn_id).read_blob(container=container, blob_path=source_path)
        #     return pl.read_json(source=data)

        # raise Exception("No handler.")
