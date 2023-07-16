from shared.utils.logging.types import LogEvent
from typing import Literal, Optional, TypeAlias

Jobs: TypeAlias = Literal["SurrogateKey", "OpenFigi", "GeneralMapping"]


class InitMapping(LogEvent):
    name: str = "InitMapping"
    job: Jobs
    size: Optional[int] = None
    product: Optional[str] = None


class MappingSuccess(LogEvent):
    name: str = "MappingSuccess"
    job: Jobs
    size: Optional[int] = None
    product: Optional[str] = None


class MapData(LogEvent):
    name: str = "MapData"
    product: Optional[str] = None
    job: Jobs
    size: Optional[int] = None


class MissingRecords(LogEvent):
    name: str = "RecordsMissing"
    job: Jobs
    size: Optional[int] = None
    product: Optional[str] = None


class RecordsCreated(LogEvent):
    name: str = "RecordsCreated"
    job: Jobs
    size: Optional[int] = None
    product: Optional[str] = None


class RecordsNotCreated(LogEvent):
    name: str = "RecordsNotCreated"
    job: Jobs
    size: Optional[int] = None
    product: Optional[str] = None


class NoMatch(LogEvent):
    name: str = "NoMatch"


class DifferentSize(LogEvent):
    name: str = "DifferentSize"
