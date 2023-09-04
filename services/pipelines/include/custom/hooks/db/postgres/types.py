from typing import Any, Dict, Literal, Sequence, TypedDict, TypeVar, TypeAlias, Type
from psycopg import sql

from pydantic import BaseModel
from typing_extensions import LiteralString

PostgresModelRecord = TypeVar("PostgresModelRecord", bound=BaseModel)


PostgresDictRecord: TypeAlias = Dict[str, Any] | object

PostgresTable: TypeAlias = str | tuple[str, str]


PostgresColumns: TypeAlias = list[str]
PostgresData: TypeAlias = Dict[str, Any] | list[Dict[str, Any]]

PostgresComposedQuery: TypeAlias = sql.SQL | sql.Composed
PostgresQuery: TypeAlias = LiteralString | PostgresComposedQuery


PostgresReturning: TypeAlias = bool | PostgresColumns


class ColumnRecord(BaseModel):
    column: str
    value: Any
