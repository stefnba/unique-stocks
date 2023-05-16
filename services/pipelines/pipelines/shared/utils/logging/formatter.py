import json
import logging
import time
from datetime import datetime

BASE_FORMAT = (
    "%(asctime)s-%(levelname)s-%(extra)s-%(name)s::[%(filename)s:%(lineno)d]::%(module)s|%(lineno)s:: %(message)s"
)

from typing import Optional

import pydantic


class Log(pydantic.BaseModel):
    message: str
    logger: str
    level: str
    timestamp: float
    extra: Optional[dict | list]
    event: Optional[str]
    created: datetime


class BaseFormatter(logging.Formatter):
    converter = time.gmtime

    def get_record_model(self, record: logging.LogRecord) -> Log:
        message = record.msg
        logger = record.name
        level = record.levelname
        timestamp = record.created
        extra = record.__dict__.get("extra", {})
        event = record.__dict__.get("event", None)
        created = record.__dict__.get("asctime", datetime.utcnow())

        return Log(
            created=created, message=message, logger=logger, level=level, timestamp=timestamp, extra=extra, event=event
        )

    def format(self, record: logging.LogRecord):
        log = self.get_record_model(record)

        logged_message = f'{log.created} | {log.level} @ {log.logger} Logger -> "{log.message}"'

        if log.event:
            logged_message += f" | event={log.event}"

        if log.extra:
            logged_message += f" | extra={log.extra}"

        return logged_message


class JSONFormatter(BaseFormatter):
    def format(self, record: logging.LogRecord):
        log = self.get_record_model(record)

        return json.dumps({"timestamp": log.timestamp, "event": log.event, "extra": log.extra, "level": log.level})
