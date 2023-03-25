from typing import Sequence

from pydantic import BaseModel


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
