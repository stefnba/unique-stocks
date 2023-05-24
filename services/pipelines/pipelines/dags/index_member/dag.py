# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import TaskInstance


@task
def extract_index_codes():
    return []


@task_group
def manage_one_index(index_code: str):
    """
    Manages downloading and processing of one index.
    """

    @task
    def ingest(index_code: str):
        from dags.index_member.eod_historical_data.jobs import ASSET_SOURCE, ingest
        from dags.index_member.path import IndexMemberPath
        from shared.hooks.data_lake import data_lake_hooks

        return data_lake_hooks.checkout(
            func=lambda: ingest(index_code),
            commit_path=IndexMemberPath.raw(source=ASSET_SOURCE, file_type="json", bin=index_code),
        )

    @task
    def transform(file_path):
        from dags.index_member.eod_historical_data.jobs import ASSET_SOURCE, transform
        from dags.index_member.path import IndexMemberPath
        from shared.hooks.data_lake import data_lake_hooks

        return data_lake_hooks.checkout(
            checkout_path=file_path,
            func=lambda data: transform(data),
            commit_path=IndexMemberPath.processed(source=ASSET_SOURCE, bin=index_code),
        )

    download_task = ingest(index_code)
    transform(download_task)


@task
def merge_all_indices(**context: TaskInstance):
    file_path = context["ti"].xcom_pull()
    print(file_path)
    print("process")


with DAG(
    dag_id="index_member",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["index", "securities"],
) as dag:
    extract_index_codes_task = extract_index_codes()
    manage_one_index.expand(index_code=extract_index_codes_task) >> merge_all_indices()
