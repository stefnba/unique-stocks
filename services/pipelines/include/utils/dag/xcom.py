from airflow.operators.python import get_current_context
from airflow.models.taskinstance import TaskInstance
from airflow.exceptions import AirflowException
from airflow.utils.context import Context

from typing import TypeVar, Any, Optional, overload, Type

R = TypeVar("R")


class XComSetter:
    @staticmethod
    def set(key: str, value: Any):
        """
        Set a XComs value. Can only be used inside a `task` function.
        """
        context = get_current_context()

        task_instance = context.get("ti")
        if isinstance(task_instance, TaskInstance):
            return task_instance.xcom_push(key=key, value=value)

        raise AirflowException(
            "Current context was requested but no context was found! Are you running within an airflow task?"
        )


class XComGetter:
    """Get XComs value outside of `task` functions to get xcom values."""

    task_id: str | list[str]
    key: str
    use_map_index: bool

    def __init__(self, task_id: str | list[str], key: str = "return_value", use_map_index=True) -> None:
        self.task_id = task_id
        self.key = key
        self.use_map_index = use_map_index

    def parse(self, context: Context):
        """Retrieve XComs key from Airflow context."""

        task_id = self.task_id
        key = self.key

        ti = context.get("task_instance")

        if isinstance(ti, TaskInstance):
            index = ti.map_index if isinstance(ti.map_index, int) else -1
            xcom_value = ti.xcom_pull(task_ids=task_id, key=key, map_indexes=index)
            return xcom_value

    def pull(self):
        """
        Retrieve a XComs key. Can only be used inside a `task` function.
        """
        context = get_current_context()

        task_instance = context.get("ti")
        if isinstance(task_instance, TaskInstance):
            index = -1

            if self.use_map_index and isinstance(task_instance.map_index, int):
                index = task_instance.map_index

            value = task_instance.xcom_pull(task_ids=self.task_id, key=self.key, map_indexes=index)

            return value

        raise AirflowException(
            "Current context was requested but no context was found! Are you running within an airflow task?"
        )

    @staticmethod
    def pull_with_template(task_id: str, key: str = "return_value", use_map_index=True) -> str:
        """Helper to build a Jinja templating string to get a XComs value."""

        map_index = "task_instance.map_index" if use_map_index else -1
        template = f"{{{{ task_instance.xcom_pull(task_ids='{task_id}', key='{key}', map_indexes={map_index}) }}}}"
        return template

    @classmethod
    def pull_now(cls, task_id: str, key: str = "return_value", use_map_index=True):
        return cls(task_id=task_id, key=key, use_map_index=use_map_index).pull()


# @overload
# def get_xcom_value(task_id: str, key: str = "return_value", *, use_map_indexes=True, return_type: Type[R]) -> R:
#     ...


# @overload
# def get_xcom_value(task_id: str, key: str = "return_value", use_map_indexes=True, return_type=None) -> None:
#     ...


# def get_xcom_value(
#     task_id: str, key: str = "return_value", use_map_indexes=True, return_type: Optional[Type[R]] = None
# ) -> R | Any:
#     """
#     Retrieve a XComs key. Can only be used inside a `task` function.
#     """

#     context = get_current_context()

#     task_instance = context.get("ti")
#     if isinstance(task_instance, TaskInstance):
#         index = -1

#         if use_map_indexes and isinstance(task_instance.map_index, int):
#             index = task_instance.map_index

#         value = task_instance.xcom_pull(task_ids=task_id, key=key, map_indexes=index)

#         return value

#     raise AirflowException(
#         "Current context was requested but no context was found! Are you running within an airflow task?"
#     )
