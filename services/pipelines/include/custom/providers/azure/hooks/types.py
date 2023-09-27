from typing import TypeAlias, TypeVar, Callable
from pydantic import BaseModel


from polars import LazyFrame, DataFrame
from duckdb import DuckDBPyRelation
import pyarrow.dataset as ds

DatasetType = TypeVar("DatasetType", LazyFrame, DataFrame, DuckDBPyRelation)


class AzureDataLakeCredentials(BaseModel):
    account_name: str
    account_url: str
    client_id: str
    secret: str
    tenant_id: str


Dataset: TypeAlias = LazyFrame | DuckDBPyRelation | ds.FileSystemDataset


DatasetTypeInput: TypeAlias = LazyFrame | DataFrame | DuckDBPyRelation | ds.FileSystemDataset
DatasetTypeReturn = DatasetTypeInput

DatasetConverter: TypeAlias = Callable[[DatasetTypeInput], DatasetType]
