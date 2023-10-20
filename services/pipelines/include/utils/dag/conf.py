from airflow.models import dagrun
from airflow.operators.python import get_current_context


def get_dag_conf() -> dict:
    """Retrieve configuration for a DAG run."""
    dag_run = get_current_context().get("dag_run")
    if isinstance(dag_run, dagrun.DagRun) and isinstance(dag_run.conf, dict):
        return dag_run.conf
    raise ValueError("Current context was requested but no context was found! Are you running within an airflow task?")
