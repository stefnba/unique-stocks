from typing import Literal, TypeAlias, TypedDict, Optional, TYPE_CHECKING


if TYPE_CHECKING:
    from utils.filesystem.data_lake.base import DataLakePathBase

Flavor = Literal["hive", "value"]


DataLakePath: TypeAlias = "str | DataLakePathBase"


class DataLakePathSerialized(TypedDict):
    format: str
    filename: Optional[str]
    directory: Optional[str]
    path: str
    container: str


class DataLakePathAzure(TypedDict):
    blob_name: str
    container_name: str
