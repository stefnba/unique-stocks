import logging

from typing import Optional

from shared.utils.logging.handlers import BaseHandler, FileHandler
from shared.utils.logging.types import LogEvent


class Logger:
    __logger: logging.Logger

    def __init__(self, name: str = "root") -> None:
        self.__logger = logging.Logger(name.upper())

    def add_handler(self, handler: BaseHandler):
        """
        Adds a new handler to Logger.

        Args:
            handler (LoggingHandler):
        """
        # handle assignment of FileHandler with respective logger name as filename
        if isinstance(handler, FileHandler) and handler.loggername_as_filename:
            handler.filename = self.__logger.name
            handler.assign_handler()

        _handler = handler.handler

        if handler.formatter:
            _handler.setFormatter(handler.formatter)

        self.__logger.addHandler(handler.handler)

    def _log(
        self,
        level: int,
        msg=None,
        extra: Optional[dict] = None,
        event: Optional[str | LogEvent] = None,
        **kwargs,
    ):
        """
        Base method to emit a log using self.__logger.
        """

        extra = {**kwargs, **(extra or {})}

        # unpack a LogEvent
        if isinstance(event, LogEvent):
            event_extra = event.dict()
            event = event_extra.pop("name")

            extra = {**extra, **event_extra}

        self.__logger.log(
            level=level,
            msg=msg,
            extra={"extra": extra, "event": event},
            stacklevel=3,
        )

    def critical(self, msg=None, event: Optional[str | LogEvent] = None, extra: Optional[dict] = None, **kwargs):
        """
        Wrapper for logging a critical event.
        """
        self._log(level=50, msg=msg, extra=extra, event=event, **kwargs)

    def error(self, msg=None, event: Optional[str | LogEvent] = None, extra: Optional[dict] = None, **kwargs):
        """
        Wrapper for logging an error.
        """
        self._log(level=40, msg=msg, extra=extra, event=event, **kwargs)

    def warn(self, msg=None, event: Optional[str | LogEvent] = None, extra: Optional[dict] = None, **kwargs):
        """
        Wrapper for logging a warning.
        """
        self._log(level=30, msg=msg, extra=extra, event=event, **kwargs)

    def info(self, msg=None, event: Optional[str | LogEvent] = None, extra: Optional[dict] = None, **kwargs):
        """
        Wrapper for logging an info event.
        """
        self._log(level=20, msg=msg, extra=extra, event=event, **kwargs)

    def debug(self, msg=None, event: Optional[str | LogEvent] = None, extra: Optional[dict] = None, **kwargs):
        """
        Wrapper for logging a debug event.
        """
        self._log(level=10, msg=msg, extra=extra, event=event, **kwargs)
