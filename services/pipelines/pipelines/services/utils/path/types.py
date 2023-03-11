from typing import Literal
from typing import Sequence

DatalakeFileTypes = Literal["csv", "json", "parquet"]

DatalakeStages = Literal["raw", "processed", "curated"]


DirectoryObject = dict[str, str | None]
DirectorySetParams = str | Sequence[str | DirectoryObject] | DirectoryObject
