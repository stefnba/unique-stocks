# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@task
def ingest():
    from dags.index.index.jobs.index import IndexJobs

    return IndexJobs.download()


@task
def transform(**context: TaskInstance):
    from dags.index.index.jobs.index import IndexJobs

    file_path = context["ti"].xcom_pull()
    print(file_path)
    return IndexJobs.transform(file_path)


@task
def curate(**context: TaskInstance):
    from dags.index.index.jobs.index import IndexJobs

    file_path: str = context["ti"].xcom_pull()

    return IndexJobs.curate(file_path)


trigger_dag_index_members = TriggerDagRunOperator(
    task_id="trigger_index_members",
    trigger_dag_id="index_members",
    wait_for_completion=False,
)

with DAG(
    dag_id="index",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["index"],
) as dag:
    ingest() >> transform() >> curate()
    # >> trigger_dag_index_members