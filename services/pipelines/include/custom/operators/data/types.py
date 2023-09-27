from typing import TypedDict, List, Literal

from typing_extensions import NotRequired


class DeltaTableOptionsDict(TypedDict):
    schema: NotRequired[dict]
    partition_by: NotRequired[List[str] | str | None]
    mode: NotRequired[Literal["error", "append", "overwrite", "ignore"]]
    overwrite_schema: NotRequired[bool]


class PyarrowOptionsDict(TypedDict):
    schema: NotRequired[dict]
