# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="indices",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["indices"],
) as dag:

    @task()
    def ingest():
        from dags.indices.jobs.eod import EodIndexJobs

        return EodIndexJobs.download_index_list()

    @task
    def process(**context: TaskInstance):
        from dags.indices.jobs.eod import EodIndexJobs

        file_path = context["ti"].xcom_pull()
        print(file_path)
        return EodIndexJobs.process_index_list(file_path)

    @task
    def curate(**context: TaskInstance):
        context["ti"].xcom_pull()
        return "Indeices downloaded"

    trigger_dag_index_members = TriggerDagRunOperator(
        task_id="trigger_index_members",
        trigger_dag_id="index_members",
        wait_for_completion=False,
    )

    ingest() >> process() >> trigger_dag_index_members >> curate()
