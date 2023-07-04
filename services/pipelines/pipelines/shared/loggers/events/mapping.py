from shared.utils.logging.types import LogEvent
from typing import Literal, Optional, TypeAlias

Jobs: TypeAlias = Literal["SurrogateKey"]


class InitMapping(LogEvent):
    name: str = "InitMapping"
    job: Jobs
    size: Optional[int] = None
    product: Optional[str] = None


class MissingRecords(LogEvent):
    name: str = "MissingRecords"
    job: Jobs
    size: Optional[int] = None
    product: Optional[str] = None


class NoMatch(LogEvent):
    name: str = "NoMatch"


class DifferentSize(LogEvent):
    name: str = "DifferentSize"
