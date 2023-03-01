from datetime import datetime

from airflow.decorators import task

from airflow import DAG

with DAG(
    dag_id="demo_dag", start_date=datetime(2022, 1, 1), schedule="0 0 * * *"
) as dag:

    @task()
    def test_airflow():
        print("Executed using Apache Airflow ✨")

    test_airflow()
