# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from airflow.models import TaskInstance


@task
def download():
    from dags.reference.countries.jobs.country_jobs import CountryJobs

    return CountryJobs.download_countries()


@task
def process(**context: TaskInstance):
    from dags.reference.countries.jobs.country_jobs import CountryJobs

    file_path: str = context["ti"].xcom_pull()

    CountryJobs.process_countries(file_path)


with DAG(
    dag_id="countries_reference",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["reference"],
) as dag:
    download() >> process()
