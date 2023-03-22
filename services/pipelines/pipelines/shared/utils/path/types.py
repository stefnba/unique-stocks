from typing import Literal, Sequence

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


class UrlPath(BaseModel):
    scheme: str
    netloc: str
    path: str
    path_components: list[str]
    params: str
    query: str
    fragment: str


class FilePath(BaseModel):
    name: str
    type: str
    extension: str
    dir_path: str
    full_path: str
    stem_name: str
    path_components: list[str]


PathParams = str | Sequence[str | None]
PathParamsOptional = PathParams | None
FileNameParams = PathParams
