import logging
from enum import Enum, EnumMeta
from typing import Dict, Generic, Literal, Optional, Type, TypedDict, TypeVar, Union

from shared.utils.logging.handlers import BaseHandler, Handlers
from shared.utils.logging.types import Levels

EventsG = TypeVar("EventsG", bound=Union[Enum, str])
ExtraG = TypeVar("ExtraG", bound=Union[TypedDict, Dict])  # type: ignore


class Logger(Generic[EventsG, ExtraG]):
    __logger: logging.Logger
    events: Type[EventsG]

    def __init__(self, name: str = "root") -> None:
        self.__logger = logging.Logger(name.upper())

    def what(self, event: EventsG):
        return 1

    def log(
        self,
        level: Levels = "INFO",
        *,
        msg: Optional[str] = None,
        event: Optional[EventsG] = None,
        extra: Optional[ExtraG] = None,
        **kwargs,
    ):
        """
        Base method to log using self.__logger.

        Args:
            msg (str): _description_
            level (Levels, optional): _description_. Defaults to "INFO".
            extra (Extra, optional): _description_. Defaults to None.
        """
        level_int = logging.getLevelName(level)
        if not isinstance(level_int, int):
            level_int = 20

        self.__logger.log(
            level=level_int,
            msg=msg or "",
            extra={
                "extra": {**kwargs, **(extra or {})},
                "event": event.name if isinstance(event, Enum) else event,
            },
        )

    def error(
        self, msg: Optional[str] = None, event: Optional[EventsG] = None, extra: Optional[ExtraG] = None, **kwargs
    ):
        self.log(level="ERROR", msg=msg, extra=extra, event=event, **kwargs)

    def info(
        self, msg: Optional[str] = None, event: Optional[EventsG] = None, extra: Optional[ExtraG] = None, **kwargs
    ):
        self.log(level="INFO", msg=msg, extra=extra, event=event, **kwargs)

    def warning(
        self, msg: Optional[str] = None, event: Optional[EventsG] = None, extra: Optional[ExtraG] = None, **kwargs
    ):
        self.log(level="WARNING", msg=msg, extra=extra, event=event, **kwargs)

    def debug(
        self, msg: Optional[str] = None, event: Optional[EventsG] = None, extra: Optional[ExtraG] = None, **kwargs
    ):
        self.log(level="DEBUG", msg=msg, extra=extra, event=event, **kwargs)

    def critical(
        self, msg: Optional[str] = None, event: Optional[EventsG] = None, extra: Optional[ExtraG] = None, **kwargs
    ):
        self.log(level="CRITICAL", msg=msg, extra=extra, event=event, **kwargs)

    def add_handler(self, handler: BaseHandler):
        """
        Adds a new handler to Logger.

        Args:
            handler (LoggingHandler):
        """

        if isinstance(handler, Handlers.File):
            handler.file_name = self.__logger.name.lower()

        _handler = handler.get_handler()

        if handler.format:
            _handler.setFormatter(handler.format)

        self.__logger.addHandler(_handler)

    def add_events(self, events: Type[EventsG]):
        """
        Add log event enum to Logger.
        """
        self.events = events
