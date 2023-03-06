# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import TaskInstance

with DAG(
    dag_id="exchange_detail",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["exchanges"],
) as dag:

    @task()
    def extract_list_exhanges_eod():
        return ["US", "LSE"]

    @task()
    def ingest_eod(**context: TaskInstance):
        from services.jobs.exchanges.eod import download_details_for_exchanges

        exchanges = context["ti"].xcom_pull(task_ids="extract_list_exhanges_eod")

        raw_file = download_details_for_exchanges(exchanges)
        return raw_file

    @task()
    def transform_eod(**context: TaskInstance):
        # from services.jobs.exchanges.eod import transform_exchanges

        file_path: str = context["ti"].xcom_pull()
        print(file_path)
        return file_path

    @task()
    def merge(**context: TaskInstance):
        file_paths: str = context["ti"].xcom_pull(task_ids=["transform_eod"])
        print(file_paths)

    (
        [
            extract_list_exhanges_eod() >> ingest_eod() >> transform_eod(),
        ]
        >> merge()
    )
