import json
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="example_template_as_python_object",
    schedule=None,
    start_date=datetime(
        2021,
        1,
        1,
    ),
    catchup=False,
    render_template_as_native_obj=False,
) as dag:

    @task
    def extract():
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        return json.loads(data_string)

    @task
    def transform(**context):
        order_data = context["ti"].xcom_pull()
        print(type(order_data))
        total_order_value = 0
        for value in order_data.values():
            total_order_value += value
        return {"total_order_value": total_order_value}

    @task
    def merge(**context):
        print(context["ti"].xcom_pull())
        return {"3333": 32222, "test": "Asdf"}

    extract() >> transform() >> merge()
