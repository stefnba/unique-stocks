from typing import TypeAlias, TypedDict, Optional
from utils.dag.xcom import XComGetter
from utils.filesystem.data_lake.base import DataLakePathBase


class DatasetPathDict(TypedDict):
    container: Optional[str]
    path: str


DatasetPath: TypeAlias = str | XComGetter | DataLakePathBase | DatasetPathDict
