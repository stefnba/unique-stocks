# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@task
def ingest_eod():
    from dags.exchanges.exchanges.jobs.eod import EodExchangeJobs

    raw_file_path = EodExchangeJobs.download_exchange_list()
    return raw_file_path


@task
def extract_codes(**context: TaskInstance):
    from dags.exchanges.exchanges.jobs.eod import EodExchangeJobs

    file_path = context["ti"].xcom_pull()
    return EodExchangeJobs.extract_exchange_codes(file_path)


@task
def process_eod_details():
    from dags.exchanges.exchanges.jobs.eod import EodExchangeJobs

    raw_file_path = EodExchangeJobs.download_exchange_list()
    return raw_file_path


@task
def merge_eod_details(**context: TaskInstance):
    file_path: str = context["ti"].xcom_pull()
    return file_path


@task
def process_eod(**context: TaskInstance):
    from dags.exchanges.exchanges.jobs.eod import EodExchangeJobs

    raw_file_path: str = context["ti"].xcom_pull(task_ids="ingest_eod")

    processed_file_path = EodExchangeJobs.process_raw_exchange_list(raw_file_path)
    return processed_file_path


@task
def ingest_iso():
    from dags.exchanges.exchanges.jobs.iso import IsoExchangeJobs

    return IsoExchangeJobs.download_exchanges()


@task
def process_iso(**context: TaskInstance):
    from dags.exchanges.exchanges.jobs.iso import IsoExchangeJobs

    raw_file_path: str = context["ti"].xcom_pull(task_ids="ingest_iso")

    return IsoExchangeJobs.process_exchanges(raw_file_path)


@task
def ingest_msk():
    from dags.exchanges.exchanges.jobs.market_stack import MarketStackExchangeJobs

    raw_file = MarketStackExchangeJobs.download_exchanges()
    return raw_file


@task
def process_msk(**context: TaskInstance):
    from dags.exchanges.exchanges.jobs.market_stack import MarketStackExchangeJobs

    raw_file_path: str = context["ti"].xcom_pull(task_ids="ingest_msk")

    processed_file_path = MarketStackExchangeJobs.process_raw_exchanges(raw_file_path)
    return processed_file_path


@task
def combine():
    return {
        "eod_exchange_path": "processed/product=exchanges/asset=exchanges/source=EodHistoricalData/y=2023/m=03/20230312_EodHistoricalData_exchanges_processed.parquet",
        "iso_exchange_path": "processed/product=exchanges/asset=exchanges/source=IsoExchanges/y=2023/m=03/20230312_IsoExchanges_exchanges_processed.parquet",
        "ms_exchange_path": "processed/product=exchanges/asset=exchanges/source=MarketStack/y=2023/m=03/20230312_MarketStack_exchanges_processed.parquet",
    }


@task
def merge(**context: TaskInstance):
    from dags.exchanges.exchanges.jobs.merge import ExchangeSources, merge_exchanges

    file_paths: ExchangeSources = {
        "eod_exchange_path": context["ti"].xcom_pull(task_ids="process_eod"),
        "iso_exchange_path": context["ti"].xcom_pull(task_ids="process_iso"),
        "ms_exchange_path": context["ti"].xcom_pull(task_ids="process_marketstack"),
    }

    merge_exchanges(file_paths)


@task
def load_into_db(**context: TaskInstance):
    context["ti"].xcom_pull()
    return "test"


trigger_exchange_securities_dag = TriggerDagRunOperator(
    task_id="trigger_exchange_securities",
    trigger_dag_id="exchange_securities",
    wait_for_completion=False,
)


with DAG(
    dag_id="exchanges",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["exchanges"],
) as dag:
    (
        [
            ingest_eod() >> process_eod(),
            ingest_iso() >> process_iso(),
            ingest_msk() >> process_msk(),
        ]
        >> merge()
        >> trigger_exchange_securities_dag
        >> load_into_db()
    )
