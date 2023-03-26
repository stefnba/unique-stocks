# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import TaskInstance


@task
def extract_index_codes(**context: TaskInstance):
    from dags.indices.index_members.jobs.index_members import IndexMembersJobs

    # file path for processed indices
    file_path: str = context["ti"].xcom_pull(dag_id="indices", task_ids="process", include_prior_dates=True)

    codes = IndexMembersJobs.extract_index_codes(file_path)
    return codes


@task_group
def one_index(index_code: str):
    """
    Manages downloading and processing of one index.
    """

    @task
    def download_members_of_index(index: str):
        from dags.indices.index_members.jobs.index_members import IndexMembersJobs

        return IndexMembersJobs.download_members_of_index(index)

    @task
    def process_members_of_index(file_path):
        from dags.indices.index_members.jobs.index_members import IndexMembersJobs

        return IndexMembersJobs.process_members_of_index(file_path)

    download_members_of_index_task = download_members_of_index(index_code)
    process_members_of_index(download_members_of_index_task)


@task
def merge_all_indices(**context: TaskInstance):
    file_path = context["ti"].xcom_pull()
    print(file_path)
    print("process")


with DAG(
    dag_id="index_members",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["indices", "securities"],
) as dag:
    extract_index_codes_task = extract_index_codes()
    one_index.expand(index_code=extract_index_codes_task) >> merge_all_indices()
