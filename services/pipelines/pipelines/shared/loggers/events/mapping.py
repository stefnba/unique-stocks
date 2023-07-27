from shared.utils.logging.types import LogEvent
from typing import Literal, Optional, TypeAlias

Jobs: TypeAlias = Literal["SurrogateKey", "OpenFigi", "GeneralMapping"]


class InitMapping(LogEvent):
    name: str = "InitMapping"
    job: Jobs
    size: Optional[int] = None
    product: Optional[str] = None


class MappingIteration(LogEvent):
    name: str = "MappingIteration"
    job: Jobs
    size: Optional[int] = None
    iteration: Optional[int] = None


class MappingSuccess(LogEvent):
    name: str = "MappingSuccess"
    job: Jobs
    size: Optional[int] = None
    product: Optional[str] = None


class MissingRecords(LogEvent):
    name: str = "RecordsMissing"
    job: Jobs
    size: Optional[int] = None
    product: Optional[str] = None


class NoMissingRecords(LogEvent):
    name: str = "NoMissingRecords"
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
    job: Jobs


class MappingResult(LogEvent):
    name: str = "MappingResult"
    matched: int
    missing: int
    job: Jobs


class DifferentSize(LogEvent):
    name: str = "DifferentSize"
    job: Jobs
