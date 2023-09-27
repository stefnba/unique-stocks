from airflow.hooks.base import BaseHook
from adlfs import AzureBlobFileSystem
import pyarrow.dataset as ds
from shared.types import DataLakeDatasetFileTypes

import polars as pl
from typing import Literal, overload
import duckdb

from utils.filesystem.path import Path, PathInput

from custom.providers.azure.hooks.types import AzureDataLakeCredentials, DatasetType

from custom.providers.azure.hooks.handlers.base import DatasetHandler
from custom.providers.azure.hooks.handlers.read.azure import AzureDatasetReadHandler
from custom.providers.azure.hooks.handlers.write.azure import AzureDatasetWriteUploadHandler

from enum import Enum


class DatasetTypeEnum(Enum):
    LazyFrame = pl.LazyFrame
    DataFrame = pl.DataFrame
    DuckDbRelation = duckdb.DuckDBPyRelation


class AzureDatasetHook(BaseHook):
    conn_id: str
    filesystem: AzureBlobFileSystem
    credentials: AzureDataLakeCredentials

    def __init__(self, conn_id: str) -> None:
        self.conn_id = conn_id

        self.credentials = self.get_conn()

        self.filesystem = AzureBlobFileSystem(
            account_name=self.credentials.account_name,
            client_id=self.credentials.client_id,
            client_secret=self.credentials.secret,
            tenant_id=self.credentials.tenant_id,
        )

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
        destination_path: PathInput,
        handler: type[DatasetHandler] = AzureDatasetWriteUploadHandler,
        **kwargs,
    ) -> str:
        """
        Save a dataset in Azure Data Lake Storage.
        The arg handler specifies the mehtod with which transfer is performend.
        """

        _handler = handler(
            path=Path.create(destination_path),
            conn_id=self.conn_id,
            filesystem=self.filesystem,
        )
        return _handler.write(dataset=dataset, **kwargs)

    @overload
    def read(
        self,
        source_path: PathInput,
        source_format: DataLakeDatasetFileTypes = "parquet",
        handler: type[DatasetHandler] = AzureDatasetReadHandler,
        *,
        dataset_type: Literal["DuckDBRel", "DuckDBLocalScan"],
        **kwargs,
    ) -> duckdb.DuckDBPyRelation:
        ...

    @overload
    def read(
        self,
        source_path: PathInput,
        source_format: DataLakeDatasetFileTypes = "parquet",
        handler: type[DatasetHandler] = AzureDatasetReadHandler,
        *,
        dataset_type: Literal["PolarsDataFrame"] = ...,
        **kwargs,
    ) -> pl.DataFrame:
        ...

    @overload
    def read(
        self,
        source_path: PathInput,
        source_format: DataLakeDatasetFileTypes = "parquet",
        handler: type[DatasetHandler] = AzureDatasetReadHandler,
        *,
        dataset_type: Literal["PolarsLocalScan", "PolarsLazyFrame"],
        **kwargs,
    ) -> pl.LazyFrame:
        ...

    @overload
    def read(
        self,
        source_path: PathInput,
        source_format: DataLakeDatasetFileTypes = "parquet",
        handler: type[DatasetHandler] = AzureDatasetReadHandler,
        *,
        dataset_type: Literal["ArrowDataset"] = ...,
        **kwargs,
    ) -> ds.FileSystemDataset:
        ...

    @overload
    def read(
        self,
        source_path: PathInput,
        **kwargs,
    ) -> duckdb.DuckDBPyRelation:
        ...

    def read(
        self,
        source_path: PathInput,
        source_format: DataLakeDatasetFileTypes = "parquet",
        handler: type[DatasetHandler] = AzureDatasetReadHandler,
        dataset_type: DatasetType = "DuckDBRel",
        **kwargs,
    ) -> duckdb.DuckDBPyRelation | pl.LazyFrame | pl.DataFrame | str | ds.FileSystemDataset:
        """
        Loads a dataset and return it.

        The method to load is specified by the `handler` arg and how the dataset is returned is specified by
        the `dataset_type` arg.


        Returns:
            Dataset either as duckdb.DuckDBPyRelation | pl.LazyFrame | pl.DataFrame | file path string depending on
            specified `dataset_type` arg.
        """

        source_path = Path.create(source_path)

        self.log.info(f"Reading dataset from {source_path.uri} with handler '{handler.__name__}'.")

        # read dataset with specified handler
        dataset = handler(
            path=source_path,
            format=source_format,
            filesystem=self.filesystem,
            conn_id=self.conn_id,
        ).read(**kwargs)

        self.log.info(
            f"Finished reading dataset from {source_path.uri} with handler '{handler.__name__}' and returning dataset as '{dataset_type}'."  # noqa: E501
        )

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
