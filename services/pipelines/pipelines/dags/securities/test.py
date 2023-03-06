# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.connection import Connection

# from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.postgres.hooks.postgres import PostgresHook

# from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="test",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["test"],
    default_args={
        "provide_context": True,
    },
) as dag:
    # create_table = PostgresOperator(
    #     postgres_conn_id="Database",
    #     task_id="create_table",
    #     sql="""
    #         SELECT * FROM securities_types
    #     """,
    # )

    @task
    def get_data():
        hook = PostgresHook(
            connection=Connection(
                extra={"cursor": "dictcursor"},
                host="unique-stocks-db-pg",
                login="admin",
                password="password",
                schema="uniquestocks",
            )
        )
        # hook = PostgresHook(postgres_conn_id="Database")
        results = hook.get_records("SELECT * FROM securities_types")
        print(results)

    get_data()
