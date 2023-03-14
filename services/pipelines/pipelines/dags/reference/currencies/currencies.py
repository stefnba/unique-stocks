# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from airflow.models import TaskInstance


@task
def download():
    from dags.reference.currencies.jobs.currency_jobs import CurrencyJobs

    return CurrencyJobs.download_currencies()


@task
def process(**context: TaskInstance):
    from dags.reference.currencies.jobs.currency_jobs import CurrencyJobs

    file_path: str = context["ti"].xcom_pull()

    CurrencyJobs.process_currencies(file_path)


with DAG(
    dag_id="currencies_reference",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["reference"],
) as dag:
    download() >> process()
