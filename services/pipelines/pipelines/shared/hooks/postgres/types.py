from typing import Any, Dict, Literal, Sequence, Tuple, TypedDict, TypeVar

from psycopg.abc import Query
from pydantic import BaseModel
from shared.utils.sql.file import QueryFile
from typing_extensions import NotRequired

QueryColumnModel = BaseModel


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
    operator: NotRequired[FilterOperator]


DbModelRecord = TypeVar("DbModelRecord", bound=BaseModel)

QueryParams = Dict[str, Any] | object
QueryInput = Query | QueryFile

DbModelSub = TypeVar("DbModelSub", bound=BaseModel)

QueryData = QueryParams | BaseModel
QueryDataMultiple = Sequence[BaseModel]

DbDictRecord = QueryParams
FilterParams = Query | list[FilterObject]


ReturningParams = Literal["ALL_COLUMNS"] | list[str]

ConflictLiteral = Literal["DO_NOTHING"]


class ConflictActionDict(TypedDict):
    column: str
    value: Any


class ConflictUpdateDict(TypedDict):
    target: list[str]
    action: list[ConflictActionDict]


ConflictParams = ConflictLiteral | ConflictUpdateDict


# Dict[str, ConflictLiteral] | ConflictLiteral | Tuple[Literal["DO_UPDATE"],  str]
