from typing import Any, Dict, Literal, Sequence, TypedDict, TypeVar

from psycopg.abc import Query
from pydantic import BaseModel


class ConnectionObject(TypedDict):
    host: str
    port: int
    user: str
    password: str
    db_name: str


class ConnectionModel(BaseModel):
    host: str
    port: int
    user: str
    password: str
    db_name: str


FilterOperator = Literal[
    "EQUAL",
    "IN",
    "HIGHER",
    "HIGHER_EQUAL",
    "LOWER",
    "LOWER_EQUAL",
    "IS_NULL",
    "NOT_NULL",
]


class FilterObject(TypedDict):
    column: str
    value: Any | None
    operator: FilterOperator


DbModelRecord = TypeVar("DbModelRecord", bound=BaseModel)

QueryParams = Dict[str, Any]

DbModelSub = TypeVar("DbModelSub", bound=BaseModel)

QueryData = QueryParams | BaseModel
QueryDataMultiple = Sequence[BaseModel]

DbDictRecord = QueryParams
FilterParams = Query | list[FilterObject]


ReturningParams = str
ConflictParams = str
