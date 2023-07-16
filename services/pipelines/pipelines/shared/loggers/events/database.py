from shared.utils.logging.types import LogEvent


class BaseEvent(LogEvent):
    query: str


class Query(BaseEvent):
    name: str = "Query"


class QueryExecution(BaseEvent):
    name: str = "QueryExecution"


class QueryResult(BaseEvent):
    name: str = "QueryResult"
