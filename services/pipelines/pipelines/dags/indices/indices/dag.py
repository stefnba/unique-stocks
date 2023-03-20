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
    from dags.indices.indices.jobs.indices import IndexJobs

    return IndexJobs.download()


@task
def process(**context: TaskInstance):
    from dags.indices.indices.jobs.indices import IndexJobs

    file_path = context["ti"].xcom_pull()
    print(file_path)
    return IndexJobs.process(file_path)


@task
def curate(**context: TaskInstance):
    from dags.indices.indices.jobs.indices import IndexJobs

    file_path: str = context["ti"].xcom_pull()

    IndexJobs.curate(file_path)

    return "Indices downloaded"


trigger_dag_index_members = TriggerDagRunOperator(
    task_id="trigger_index_members",
    trigger_dag_id="index_members",
    wait_for_completion=False,
)

with DAG(
    dag_id="indices",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["indices"],
) as dag:
    ingest() >> process() >> trigger_dag_index_members >> curate()
