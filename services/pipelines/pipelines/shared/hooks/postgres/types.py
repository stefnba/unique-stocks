from typing import Any, Dict, Literal, Sequence, TypedDict, TypeVar, TypeAlias

from psycopg.abc import Query
from pydantic import BaseModel
from shared.utils.sql.file import QueryFile
from typing_extensions import NotRequired, LiteralString

QueryColumnModel: TypeAlias = BaseModel


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

QueryParams: TypeAlias = Dict[str, Any] | object
QueryInput: TypeAlias = Query | QueryFile

DbModelSub = TypeVar("DbModelSub", bound=BaseModel)

QueryData: TypeAlias = QueryParams | BaseModel
QueryDataMultiple = Sequence[BaseModel]

DbDictRecord = QueryParams
FilterParams = Query | list[FilterObject]


ReturningParams = Literal["ALL_COLUMNS"] | list[str]

ConflictLiteral = Literal["DO_NOTHING"]


class ConflictActionValueDict(TypedDict):
    column: str
    value: Any


class ConflictActionExcludedDict(TypedDict):
    column: str
    excluded: LiteralString


class ConflictUpdateDict(TypedDict):
    target: list[str]
    action: list[ConflictActionValueDict | ConflictActionExcludedDict]


ConflictParams: TypeAlias = ConflictLiteral | ConflictUpdateDict | str
