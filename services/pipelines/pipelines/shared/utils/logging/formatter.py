import logging
from logging import LogRecord
import json
from string import Template, Formatter
from typing import Optional

from shared.utils.logging.utils import get_airflow_context


FORMAT = "[${asctime}] ${name}:${levelname} (${filename}:${lineno}) :: ${event} - ${message}"


class BaseFormatter(logging.Formatter):
    exclude_keys: Optional[list[str]] = None

    def __init__(
        self,
    ) -> None:
        super().__init__()

    def transform_record(self, record: LogRecord, drop_keys: list[str] = []) -> dict:
        """
        Makes necessary transformation to log record.
        If self.exclude_keys is specified, ignore these keys.
        """

        airflow_context = get_airflow_context()

        record_body = {
            **record.__dict__,
            "asctime": self.formatTime(record),
            "message": record.__dict__["msg"],
            "dag_id": airflow_context["dag_id"],
            "task_id": airflow_context["task_id"],
            "run_id": airflow_context["run_id"],
        }

        if not self.exclude_keys:
            return record_body

        return {k: record_body[k] for k in record_body if k not in self.exclude_keys}


class TextFormatter(BaseFormatter):
    exclude_keys = [
        "args",
        "levelno",
        "module",
        "created",
        "msg",
        "relativeCreated",
        "thread",
        "msecs",
        "threadName",
        "processName",
        "process",
    ]

    def template(self, text=FORMAT, *, record: dict) -> str:
        template = Template(text)
        used_keys = [i[1] for i in Formatter().parse(text) if i[1] is not None]
        unused_items = {k: record[k] for k in record if k not in used_keys}

        return f"{template.safe_substitute(record)}\n{json.dumps(unused_items, default=str, indent=4)}"

    def format(self, record: LogRecord) -> str:
        _record = self.transform_record(record)
        return self.template(record=_record)


class JsonFormatter(BaseFormatter):
    def format(self, record: LogRecord) -> str:
        return json.dumps(self.transform_record(record), default=str)


class JsonFormatterAirflow(logging.Formatter):
    """
    JSON formatter for sending Airflow logs to centralized logging.
    """

    def format(self, record: LogRecord) -> str:
        from airflow.models.taskinstance import TaskInstance

        record_dict = record.__dict__

        args = record_dict.get("args", [])

        task_id: str | None = None
        dag_id: str | None = None
        run_id: str | None = None
        map_index: str | None = None

        for arg in args:
            if isinstance(arg, TaskInstance):
                task_id = str(arg.task_id)
                dag_id = str(arg.dag_id)
                run_id = str(arg.run_id)

        # specify message
        record_dict["message"] = record.getMessage()

        airflow_context = get_airflow_context()

        record_dict["dag_id"] = airflow_context["dag_id"] or dag_id
        record_dict["task_id"] = airflow_context["task_id"] or task_id
        record_dict["run_id"] = airflow_context["run_id"] or run_id
        record_dict["map_index"] = airflow_context["map_index"] or map_index

        return json.dumps(record_dict, default=str)
