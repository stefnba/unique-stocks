from typing import Optional
from shared.utils.logging.types import LogEvent


class AddInit(LogEvent):
    name: str = "AddInit"
    table: str | tuple[str, str]
    length: int


class CopyInit(LogEvent):
    name: str = "CopyInit"
    table: str | tuple[str, str]
    columns: list[str]


class CopySuccess(LogEvent):
    name: str = "CopySuccess"
    table: str | tuple[str, str]
    columns: list[str]
    row_count: int


class CopyInsertFromTemp(LogEvent):
    name: str = "CopyInsertFromTemp"
    table: str | tuple[str, str]
    columns: list[str]
    query: str


class BaseEvent(LogEvent):
    query: str


class Query(BaseEvent):
    name: str = "Query"


class QueryExecution(BaseEvent):
    name: str = "QueryExecution"
    table: Optional[str | tuple[str, str]] = None


class QueryResult(BaseEvent):
    name: str = "QueryResult"
    length: Optional[int]
    table: Optional[str | tuple[str, str]] = None
