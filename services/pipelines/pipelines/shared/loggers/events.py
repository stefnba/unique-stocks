from enum import Enum, auto


class Transform(Enum):
    SUCCESS = auto()
    MULTIPLE_RECORDS = auto()


class Database(Enum):
    QUERY_EXECUTION = auto()
    QUERY = auto()
    QUERY_RESULT = auto()


class Mapping(Enum):
    NO_MATCH = auto()
    DIFFERENT_SIZE = auto()


class Airflow(Enum):
    SUCCESS = auto()


class Api(Enum):
    REQUEST_INIT = auto()
    SUCCESS = auto()
    TIMEOUT = auto()
    ERROR = auto()


class DataLake(Enum):
    DOWNLOAD_INIT = auto()
    DOWNLOAD_SUCCESS = auto()
    DOWNLOAD_ERROR = auto()
    UPLOAD_INIT = auto()
    UPLOAD_SUCCESS = auto()
    UPLOAD_ERROR = auto()
