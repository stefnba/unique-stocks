from airflow.operators.python import get_current_context
from airflow.models.taskinstance import TaskInstance
from airflow.exceptions import AirflowException

from typing import TypeVar, Any, Optional, overload, Type

R = TypeVar("R")


@overload
def get_xcom_value(task_id: str, key: str = "return_value", *, return_type: Type[R]) -> R:
    ...


@overload
def get_xcom_value(task_id: str, key: str = "return_value", return_type=None) -> None:
    ...


def get_xcom_value(task_id: str, key: str = "return_value", return_type: Optional[Type[R]] = None) -> R | Any:
    context = get_current_context()

    task_instance = context.get("ti")
    if isinstance(task_instance, TaskInstance):
        index = task_instance.map_index  # if no mapped task, map_index is -1

        value = task_instance.xcom_pull(task_ids=task_id, key=key, map_indexes=index if isinstance(index, int) else -1)

        return value

    raise AirflowException(
        "Current context was requested but no context was found! Are you running within an airflow task?"
    )
