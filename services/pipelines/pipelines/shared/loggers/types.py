from typing import TypedDict

from typing_extensions import NotRequired


class ApiExtra(TypedDict):
    url: str
    method: str
    status: NotRequired[int]
    message: NotRequired[str]


class DatabaseExtra(TypedDict):
    query: str
