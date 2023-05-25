from typing import Any, TypedDict

from typing_extensions import NotRequired


class ApiExtra(TypedDict):
    url: str
    method: str
    status: NotRequired[int]
    message: NotRequired[str]


class DataLakeExtra(TypedDict):
    format: NotRequired[str]
    path: NotRequired[str]
    handler: NotRequired[str | None]


class DatabaseExtra(TypedDict):
    query: NotRequired[str]
    table: NotRequired[str]
    data: NotRequired[Any]
