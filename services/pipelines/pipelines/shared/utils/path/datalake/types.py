from typing import Literal

from pydantic import BaseModel

DatalakeFileTypes = Literal["csv", "json", "parquet"]
DatalakeDirParams = str | list[str]


class DatalakeDate(BaseModel):
    year: str
    month: str
    day: str
    hour: str
    minute: str
    second: str
