# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from include.jobs.exchanges.eod import download_exchanges


with DAG(
    dag_id="exchange_list",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["exchanges"],
) as dag:

    @task()
    def ingest_eod():
        # test()
        raw_file = download_exchanges()
        return raw_file

    @task()
    def transform_eod(**context):
        a = context["ti"].xcom_pull(task_ids="ingest_eod")
        print("arg is", a)

    @task()
    def ingest_iso():
        test = "3333"
        return test

    @task()
    def transform_iso():
        test = "3333"
        return test

    @task()
    def ingest_marketstack():
        test = "3333"
        return test

    @task()
    def transform_marketstack():
        test = "3333"
        return test

    @task()
    def merge():
        test = "3333"
        return test

    @task()
    def load_db():
        test = "3333"
        return test

    (
        [
            ingest_eod() >> transform_eod(),
            ingest_iso() >> transform_iso(),
            ingest_marketstack() >> transform_marketstack(),
        ]
        >> merge()
        >> load_db()
    )
