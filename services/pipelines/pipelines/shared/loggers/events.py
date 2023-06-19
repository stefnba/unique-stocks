from shared.utils.logging import LogEventCollection, LogEvent, CustomLogEvent


class Transform(LogEventCollection):
    SUCCESS = LogEvent
    MULTIPLE_RECORDS = LogEvent


class Database(LogEventCollection):
    QUERY_EXECUTION = LogEvent
    QUERY = LogEvent
    QUERY_RESULT = LogEvent


class MappingTest(CustomLogEvent):
    whatever: str


class Mapping(LogEventCollection):
    NO_MATCH = LogEvent
    DIFFERENT_SIZE = LogEvent
    WHATEVER = MappingTest


class Airflow(LogEventCollection):
    SUCCESS = LogEvent


class Api(LogEventCollection):
    REQUEST_INIT = LogEvent
    SUCCESS = LogEvent
    TIMEOUT = LogEvent
    ERROR = LogEvent


class DataLake(LogEventCollection):
    DOWNLOAD_INIT = LogEvent
    DOWNLOAD_SUCCESS = LogEvent
    DOWNLOAD_ERROR = LogEvent
    UPLOAD_INIT = LogEvent
    UPLOAD_SUCCESS = LogEvent
    UPLOAD_ERROR = LogEvent
