# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import TaskInstance


@task
def extract_exchange_codes(**context: TaskInstance):
    from services.jobs.indices.eod import EodIndexJobs

    # file path for processed exchanges
    file_path: str = context["ti"].xcom_pull(dag_id="exchanges", task_ids="merge", include_prior_dates=True)

    codes = EodIndexJobs.extract_index_codes(file_path)
    return codes


@task_group
def manage_one_exchange(exchange_code: str):
    """
    Manages downloading and processing securities of one exchange
    """

    @task
    def download_securities_of_exchange(exchange: str):
        from services.jobs.indices.eod import EodIndexJobs

        return EodIndexJobs.download_members_of_index(exchange)

    @task
    def process_securities_of_exchange(file_path):
        from services.jobs.indices.eod import EodIndexJobs

        return EodIndexJobs.process_members_of_index(file_path)

    download_securities_of_exchange_task = download_securities_of_exchange(exchange_code)
    process_securities_of_exchange(download_securities_of_exchange_task)


@task
def merge_all_exchange_securities(**context: TaskInstance):
    file_path = context["ti"].xcom_pull()
    print(file_path)
    print("process")


with DAG(
    dag_id="exchange_members",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["exchanges", "securities"],
) as dag:
    extract_exchange_codes_task = extract_exchange_codes()

    manage_one_exchange.expand(exchange_code=extract_exchange_codes_task) >> merge_all_exchange_securities()
