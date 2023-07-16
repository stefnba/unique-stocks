from shared.config import CONFIG
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
