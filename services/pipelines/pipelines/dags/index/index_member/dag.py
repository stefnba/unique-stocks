# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import TaskInstance


@task
def extract_index_codes(**context: TaskInstance):
    from dags.index.index_member.jobs.index_member import IndexMembersJobs

    # file path for processed index
    file_path: str = context["ti"].xcom_pull(dag_id="index", task_ids="curate", include_prior_dates=True)
    print("test", file_path)
    return IndexMembersJobs.extract_index_codes(file_path)


@task_group
def one_index(index_code: str):
    """
    Manages downloading and processing of one index.
    """

    @task
    def download(index: str):
        from dags.index.index_member.jobs.index_member import IndexMembersJobs

        return IndexMembersJobs.download(index)

    @task
    def transform(file_path):
        from dags.index.index_member.jobs.index_member import IndexMembersJobs

        return IndexMembersJobs.transform(file_path)

    download_task = download(index_code)
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
    one_index.expand(index_code=extract_index_codes_task) >> merge_all_indices()
