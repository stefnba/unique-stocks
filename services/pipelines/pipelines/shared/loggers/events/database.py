from shared.utils.logging.types import LogEvent


class Query(LogEvent):
    name: str = "Query"


class QueryExecution(LogEvent):
    name: str = "QueryExecution"


class QueryResult(LogEvent):
    name: str = "QueryResult"
