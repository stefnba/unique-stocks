from typing import Literal, Sequence
from pydantic import BaseModel

DatalakeFileTypes = Literal["csv", "json", "parquet"]

DatalakeStages = Literal["raw", "processed", "curated"]


DirectoryObject = dict[str, str | None]
DirectorySetParams = str | Sequence[str | DirectoryObject] | DirectoryObject


class FilePath(BaseModel):
    name: str
    type: str
    path: str
    path_components: list[str]
    extension: str


PathArgs = str | list[str | None] | None
