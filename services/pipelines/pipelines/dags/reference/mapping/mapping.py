# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import TaskInstance


@task
def upload():
    from dags.reference.mapping.jobs.mapping import MappingJobs

    return MappingJobs.upload()


@task
def process(**context: TaskInstance):
    from dags.reference.mapping.jobs.mapping import MappingJobs

    file_path: str = context["ti"].xcom_pull()

    return MappingJobs.process(file_path)


with DAG(
    dag_id="mapping_reference",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["reference"],
) as dag:
    upload() >> process()
