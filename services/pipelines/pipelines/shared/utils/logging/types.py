from typing import Literal, Optional
from pydantic import BaseModel

Levels = Literal["INFO", "ERROR", "CRITICAL", "WARNING", "DEBUG"]
FileHandlerFormats = Literal[
    "JSON",
    "TEXT",
]
Extra = Optional[object | list]


class BaseLogEvent(BaseModel):
    name: str | None

    def __name__(self) -> str:
        return str(self.__name__)


class CustomLogEvent(BaseLogEvent):
    name: Optional[str] = None


class LogEvent(BaseLogEvent):
    pass


class LogEventCollection:
    pass
