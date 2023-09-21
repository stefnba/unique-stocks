from typing import TypeAlias, TypedDict, Optional, List, Literal
from utils.dag.xcom import XComGetter
from utils.filesystem.data_lake.base import DataLakePathBase
from typing_extensions import NotRequired


class DatasetPathDict(TypedDict):
    container: Optional[str]
    path: str


DatasetPath: TypeAlias = str | XComGetter | DataLakePathBase | DatasetPathDict


class DeltaTableOptionsDict(TypedDict):
    schema: NotRequired[dict]
    partition_by: NotRequired[List[str] | str | None]
    mode: NotRequired[Literal["error", "append", "overwrite", "ignore"]]
    overwrite_schema: NotRequired[bool]


class PyarrowOptionsDict(TypedDict):
    schema: NotRequired[dict]
