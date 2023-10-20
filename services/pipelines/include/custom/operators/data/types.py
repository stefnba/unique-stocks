from typing import List, Literal, TypedDict

from typing_extensions import NotRequired


class DeltaTableOptionsDict(TypedDict):
    schema: NotRequired[dict]
    partition_by: NotRequired[List[str] | str | None]
    mode: NotRequired[Literal["error", "append", "overwrite", "ignore"]]
    overwrite_schema: NotRequired[bool]


class PyarrowOptionsDict(TypedDict):
    schema: NotRequired[dict]
