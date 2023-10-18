from airflow.hooks.base import BaseHook
from adlfs import AzureBlobFileSystem
import pyarrow.dataset as ds
from shared.types import DataLakeDatasetFileTypes

import polars as pl
from typing import overload, Optional, Callable
import duckdb

from utils.filesystem.path import Path, PathInput

from custom.providers.azure.hooks.types import AzureDataLakeCredentials, DatasetType, DatasetConverter, DatasetTypeInput
from custom.providers.azure.hooks import converters, handlers


from custom.providers.azure.hooks.handlers.base import DatasetHandler
from custom.providers.azure.hooks.handlers.read.azure import AzureDatasetReadHandler
from custom.providers.azure.hooks.handlers.write.azure import AzureDatasetWriteUploadHandler


DatasetConverters = converters
DatasetHandlers = handlers.DatasetHandlers


class AzureDatasetHook(BaseHook):
    conn_id: str
    filesystem: AzureBlobFileSystem
    credentials: AzureDataLakeCredentials
    storage_options: dict

    def __init__(self, conn_id: str) -> None:
        self.conn_id = conn_id

        self.credentials = self.get_conn()

        self.filesystem = AzureBlobFileSystem(
            account_name=self.credentials.account_name,
            client_id=self.credentials.client_id,
            client_secret=self.credentials.secret,
            tenant_id=self.credentials.tenant_id,
        )

        self.storage_options = {
            "account_name": self.credentials.account_name,
            "client_id": self.credentials.client_id,
            "client_secret": self.credentials.secret,
            "tenant_id": self.credentials.tenant_id,
        }

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

        destination_path = Path.create(destination_path)

        self.log.info(
            f"Writing dataset of type {type(dataset)} to '{destination_path.uri}' using '{handler.__name__}'."
        )

        _handler = handler(
            path=destination_path,
            conn_id=self.conn_id,
            filesystem=self.filesystem,
            storage_options=self.storage_options,
        )
        return _handler.write(dataset=dataset, **kwargs)

    @overload
    def read(
        self,
        source_path: PathInput,
        source_format: Optional[DataLakeDatasetFileTypes] = ...,
        handler: type[DatasetHandler] = ...,
        *,
        dataset_converter: DatasetConverter,
        **kwargs,
    ) -> DatasetType:
        ...

    @overload
    def read(
        self,
        source_path: PathInput,
        source_format: Optional[DataLakeDatasetFileTypes] = ...,
        handler: type[DatasetHandler] = ...,
        **kwargs,
    ) -> duckdb.DuckDBPyRelation:
        ...

    def read(
        self,
        source_path: PathInput,
        source_format: Optional[DataLakeDatasetFileTypes] = "parquet",
        handler: type[DatasetHandler] = AzureDatasetReadHandler,
        dataset_converter: Optional[Callable[[DatasetTypeInput], DatasetType]] = None,
        **kwargs,
    ) -> DatasetType | duckdb.DuckDBPyRelation:
        """
        Loads a dataset and returns it.

        The method to load is specified by the `handler` arg and how the dataset is returned is specified by
        the `dataset_converter` arg.
        """

        source_path = Path.create(source_path)

        self.log.info(f"Reading dataset from '{source_path.uri}' with handler '{handler.__name__}'.")

        # read dataset with specified handler
        dataset = handler(
            path=source_path,
            format=source_format,
            filesystem=self.filesystem,
            conn_id=self.conn_id,
            storage_options=self.storage_options,
        ).read(**kwargs)

        self.log.info(
            f"Finished reading dataset from '{source_path.uri}' with handler '{handler.__name__}' and returning dataset as '{(dataset_converter or converters.DuckDbRelation).__name__}'."
        )

        # convert dataset to specified return format
        if dataset_converter:
            return dataset_converter(dataset)

        return converters.DuckDbRelation(dataset=dataset)
