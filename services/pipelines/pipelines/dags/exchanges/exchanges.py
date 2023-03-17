# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import TaskInstance


@task()
def ingest_eod():
    from dags.exchanges.jobs.eod import EodExchangeJobs

    raw_file_path = EodExchangeJobs.download_exchange_list()
    return raw_file_path


@task
def extract_codes(**context: TaskInstance):
    from dags.exchanges.jobs.eod import EodExchangeJobs

    file_path = context["ti"].xcom_pull()
    return EodExchangeJobs.extract_exchange_codes(file_path)


@task()
def ingest_eod_details(**context: TaskInstance):
    from dags.exchanges.jobs.eod import EodExchangeJobs

    exchange_code: str = context["ti"].xcom_pull()

    EodExchangeJobs.download_exchange_details(exchange_code)

    raw_file_path = EodExchangeJobs.download_exchange_list()
    return raw_file_path


@task()
def process_eod_details():
    from dags.exchanges.jobs.eod import EodExchangeJobs

    raw_file_path = EodExchangeJobs.download_exchange_list()
    return raw_file_path


@task()
def merge_eod_details(**context: TaskInstance):
    file_path: str = context["ti"].xcom_pull()
    return file_path


@task()
def process_eod(**context: TaskInstance):
    from dags.exchanges.jobs.eod import EodExchangeJobs

    raw_file_path: str = context["ti"].xcom_pull(task_ids="ingest_eod")

    processed_file_path = EodExchangeJobs.process_raw_exchange_list(raw_file_path)
    return processed_file_path


@task()
def ingest_iso():
    from dags.exchanges.jobs.iso import IsoExchangeJobs

    return IsoExchangeJobs.download_exchanges()


@task()
def process_iso(**context: TaskInstance):
    from dags.exchanges.jobs.iso import IsoExchangeJobs

    raw_file_path: str = context["ti"].xcom_pull(task_ids="ingest_iso")

    return IsoExchangeJobs.process_exchanges(raw_file_path)


@task()
def ingest_marketstack():
    from dags.exchanges.jobs.market_stack import MarketStackExchangeJobs

    raw_file = MarketStackExchangeJobs.download_exchanges()
    return raw_file


@task()
def process_marketstack(**context: TaskInstance):
    from dags.exchanges.jobs.market_stack import MarketStackExchangeJobs

    raw_file_path: str = context["ti"].xcom_pull(task_ids="ingest_marketstack")

    processed_file_path = MarketStackExchangeJobs.process_raw_exchanges(raw_file_path)
    return processed_file_path


@task()
def merge(**context: TaskInstance):
    file_paths: str = context["ti"].xcom_pull()
    print(file_paths)
    return file_paths


@task()
def load_into_db(**context: TaskInstance):
    file_paths: str = context["ti"].xcom_pull()
    return "test"


with DAG(
    dag_id="exchanges",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["exchanges"],
) as dag:
    process_eod_task = process_eod()
    # merge_eod_details_task = merge_eod_details()

    (
        [
            ingest_eod() >> process_eod_task,
            ingest_iso() >> process_iso(),
            ingest_marketstack() >> process_marketstack(),
        ]
        >> merge()
        >> load_into_db()
    )

    # process_eod_task >> extract_codes() >> ingest_eod_details() >> process_eod_details() >> merge_eod_details_task
