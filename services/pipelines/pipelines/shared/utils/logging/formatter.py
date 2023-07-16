import logging
from logging import LogRecord
import json
from string import Template
from shared.config import CONFIG


FORMAT = '${asctime} ${name}:${levelname} :: "${message}" :: {event}\n\t ${extra}'

LOG_RECORD_KEYS = [
    "name",
    "levelname",
    "event",
    "pathname",
    "filename",
    "module",
    "lineno",
    "funcName",
    "created",
    "extra",
    "service",
]


class BaseFormatter(logging.Formatter):
    def __init__(
        self,
    ) -> None:
        super().__init__()

    def transform_record(self, record: LogRecord, drop_keys: list[str] = []) -> dict:
        """
        Makes necessary transformation to log record.
        """

        airflow_context = get_airflow_context()

        return {
            # **{key: record.__dict__.get(key) for key in LOG_RECORD_KEYS},
            **record.__dict__,
            "asctime": self.formatTime(record),
            "message": record.__dict__["msg"],
            "dag_id": airflow_context["dag_id"],
            "task_id": airflow_context["task_id"],
            "run_id": airflow_context["run_id"],
        }


class TextFormatter(BaseFormatter):
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

    def format(self, record: LogRecord) -> str:
        _record = self.transform_record(record)
        return self.template(record=_record)


class JsonFormatter(BaseFormatter):
    def format(self, record: LogRecord) -> str:
        return json.dumps(self.transform_record(record), default=str)


from typing import TypedDict


class AirflowContext(TypedDict):
    dag_id: str | None
    task_id: str | None
    run_id: str | None
    map_index: str | None


def get_airflow_context() -> AirflowContext:
    if CONFIG.app.env == "Development":
        return {"dag_id": None, "task_id": None, "run_id": None, "map_index": None}

    from airflow.exceptions import AirflowException

    try:
        from airflow.operators.python import get_current_context

        context = get_current_context()

        dag = context.get("dag")
        task = context.get("task")

        return {
            "dag_id": dag.dag_id if dag else None,
            "task_id": task.task_id if task else None,
            "run_id": context.get("run_id"),
            "map_index": None,
            # "map_index": context.get("map_index"),
        }
    except AirflowException:
        return {"dag_id": None, "task_id": None, "run_id": None, "map_index": None}


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


# log_filename_template = dag_id={{ ti.dag_id }}/run_id={{ ti.run_id }}/task_id={{ ti.task_id }}/{%% if ti.map_index >= 0 %%}map_index={{ ti.map_index }}/{%% endif %%}attempt={{ try_number }}.log
