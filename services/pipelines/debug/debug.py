# %%
from airflow.hooks.base import BaseHook
from adlfs import AzureBlobFileSystem

import pyarrow.dataset as ds
from shared.types import DataLakeDataFileTypes
from settings import config_settings
from typing import Any, Optional
import polars as pl


import pyarrow as pa
from typing import Optional, TypeAlias, Literal, overload, cast
import re
from shared.types import DataLakeDataFileTypes
from azure.storage.blob import ContainerClient, BlobServiceClient, BlobClient
import duckdb
from utils.filesystem.path import TempDirPath, TempFilePath
from settings import config_settings

from azure.identity import DefaultAzureCredential

SourcePath: TypeAlias = str | list[str]
LoadHandler = Literal["ArrowDataset", "DuckDBRel"]
SinkHandler = Literal["ArrowDataset", "PolarsSinkUpload", "Upload"]
DatasetType = Literal["PolarsLazyFrame", "PolarsDataFrame", "DuckDBRel", "PolarsLocalScan", "DuckDBLocalScan"]


# ds.write_dataset(
#     dataset,
#     base_dir="dataset",
#     existing_data_behavior="overwrite_or_ignore",
#     basename_template="testasdfsdf_{i}",
#     partitioning=partitioning(schema=pa.schema([("security_type", pa.string())]))
#     # partitioning=["security_type", "country"],
#     # partitioning_flavor="hive",
# )


account_name = "uniquestocks"


# %%
from adlfs import AzureBlobFileSystem
from typing import cast

# path = "zone=processed/product=security/exchange=XETRA/source=EodHistoricalData/**.parquet"

filesystem = AzureBlobFileSystem(account_name="uniquestocks", anon=False)


# path = [
#     "zone=processed/product=security/exchange=XETRA/source=EodHistoricalData/year=2023/month=07/**.parquet",
#     "zone=processed/product=security/exchange=XETRA/source=EodHistoricalData/year=2023/month=07/day=26/20230726_122214__EodHistoricalData__security__XETRA__processed.parquet",
# ]
# # path = "zone=processed/product=security/exchange=XETRA/source=EodHistoricalData/year=2023/month=07/**.parquet"
# container = "data-lake"
# found = paths(path, "data-lake")

# print(len(found))

# %%
