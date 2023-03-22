# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import TaskInstance


@task
def ingest():
    from dags.reference.timezones.jobs.timezone import TimezoneJobs

    return TimezoneJobs.download()


@task
def process(**context: TaskInstance):
    from dags.reference.timezones.jobs.timezone import TimezoneJobs

    file_path: str = context["ti"].xcom_pull()
    return TimezoneJobs.process(file_path)


with DAG(
    dag_id="timezone",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["reference"],
) as dag:
    ingest() >> process()


if __name__ == "__main__":
    dag.clear()
    dag.test()
