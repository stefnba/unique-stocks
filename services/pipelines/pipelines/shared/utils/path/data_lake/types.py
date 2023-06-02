from typing import Literal

from pydantic import BaseModel

DatalakeFileTypes = Literal["csv", "json", "parquet", "zip"]
DatalakeDirParams = str | list[str]

DataLakePathPattern = str | list[str]
DataLakePathVersions = Literal["current", "history"]


class DatalakeDate(BaseModel):
    year: str
    month: str
    day: str
    hour: str
    minute: str
    second: str
