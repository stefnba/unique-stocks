# pylint: disable=W0106:expression-not-assigned
# pyright: reportUnusedExpression=false
from datetime import datetime


import requests
from airflow import DAG
from airflow.decorators import task
from include.test import test

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
        test()
        r = requests.get(
            "https://gist.githubusercontent.com/rnirmal/\
                e01acfdaf54a6f9b24e91ba4cae63518/raw/\
                    6b589a5c5a851711e20c5eb28f9d54742d1fe2dc/datasets.csv"
        )
        print(r.text)

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
