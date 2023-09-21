from typing import Optional, TypeAlias, Literal
from pydantic import BaseModel
from utils.filesystem.data_lake.base import DataLakePathBase


class AzureDataLakeCredentials(BaseModel):
    account_name: str
    account_url: str
    client_id: str
    secret: str
    tenant_id: str


DatasetReadPath: TypeAlias = str | list[str] | DataLakePathBase
DatasetWritePath: TypeAlias = str | DataLakePathBase
DatasetType = Literal[
    "PolarsLazyFrame", "PolarsDataFrame", "DuckDBRel", "PolarsLocalScan", "DuckDBLocalScan", "ArrowDataset"
]
