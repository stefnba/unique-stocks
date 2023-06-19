import logging
from logging import LogRecord
import json
from string import Template

FORMAT = '${time} ${name}:${levelname} :: "${msg}" :: {event}\n\t ${extra}'

LOG_RECORD_KEYS = [
    "name",
    "msg",
    "levelname",
    "event",
    "pathname",
    "filename",
    "module",
    "lineno",
    "funcName",
    "created",
    "extra",
]


class BaseFormatter(logging.Formatter):
    def __init__(
        self,
    ) -> None:
        super().__init__()

    def template(self, text=FORMAT, *, record: dict) -> str:
        """_summary_

        Args:
            text (_type_, optional): _description_. Defaults to FORMAT.
            record (dict, optional): _description_. Defaults to {}.

        Returns:
            str: _description_
        """
        template = Template(text)
        return template.safe_substitute(record)

    def transform_record(self, record: LogRecord) -> dict:
        """
        Makes necessary transformation to log record.
        """
        # exclude fields not relevant

        return {**{key: record.__dict__[key] for key in LOG_RECORD_KEYS}, "time": self.formatTime(record)}

    def format(self, record: LogRecord) -> str:
        # print(super().format(record))
        # print(self.formatTime(record))
        _record = self.transform_record(record)
        return self.template(record=_record)


class TextFormatter(BaseFormatter):
    pass


class JsonFormatter(BaseFormatter):
    def format(self, record: LogRecord) -> str:
        return json.dumps(self.transform_record(record), default=str)
