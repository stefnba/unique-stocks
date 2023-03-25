# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import TaskInstance


@task
def extract_exchange_codes(**context: TaskInstance):
    from dags.exchanges.exchange_securities.jobs.eod import EodExchangeSecurityJobs

    # file path for processed exchanges
    file_path: str = context["ti"].xcom_pull(dag_id="exchanges", task_ids="process_eod", include_prior_dates=True)

    # codes = EodExchangeJobs.extract_index_codes(file_path)
    return ["XETRA", "SW"]


@task_group
def manage_one_exchange(exchange_code: str):
    """
    Manages downloading and processing securities of one exchange
    """

    @task
    def download_securities_of_exchange(exchange: str):
        from dags.exchanges.exchange_securities.jobs.eod import EodExchangeSecurityJobs

        return EodExchangeSecurityJobs.download_securities(exchange)

    @task
    def process_securities_of_exchange(file_path: str):
        from dags.exchanges.exchange_securities.jobs.eod import EodExchangeSecurityJobs

        return EodExchangeSecurityJobs.process_securities(exchange_code, file_path)

    download_securities_of_exchange_task = download_securities_of_exchange(exchange_code)
    process_securities_of_exchange(download_securities_of_exchange_task)


@task
def merge_all_exchange_securities(**context: TaskInstance):
    file_path = context["ti"].xcom_pull()
    print(file_path)
    print("process")


with DAG(
    dag_id="exchange_securities",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["exchanges", "securities"],
) as dag:
    extract_exchange_codes_task = extract_exchange_codes()

    manage_one_exchange.expand(exchange_code=extract_exchange_codes_task) >> merge_all_exchange_securities()
