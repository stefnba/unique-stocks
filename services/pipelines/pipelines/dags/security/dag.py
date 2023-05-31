# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime
from typing import TypedDict

from airflow import DAG
from airflow.decorators import task, task_group


class ExchangeDataset(TypedDict):
    exchange_code: str
    file_path: str


@task
def extract_exchange_codes():
    from dags.security.eod_historical_data.jobs import extract

    return extract()


@task_group
def manage_one_exchange(exchange_code: str):
    """
    Manages downloading and processing securities of one exchange.
    """

    @task
    def ingest(exchange: str) -> ExchangeDataset:
        from dags.security.eod_historical_data.jobs import ASSET_SOURCE, ingest
        from dags.security.path import SecurityPath
        from shared.hooks.data_lake import data_lake_hooks

        file_path = data_lake_hooks.checkout(
            func=lambda: ingest(exchange),
            commit_path=SecurityPath.raw(source=ASSET_SOURCE, bin=exchange, file_type="json"),
        )

        return {
            "exchange_code": exchange,
            "file_path": file_path,
        }

    @task
    def transform(dataset: ExchangeDataset):
        from dags.security.eod_historical_data.jobs import ASSET_SOURCE, transform
        from dags.security.path import SecurityPath
        from shared.hooks.data_lake import data_lake_hooks

        return data_lake_hooks.checkout(
            checkout_path=dataset["file_path"],
            func=lambda data: transform(data),
            commit_path=SecurityPath.processed(source=ASSET_SOURCE, bin=dataset["exchange_code"]),
        )

    @task
    def extract_security(file_path: str):
        from dags.security.eod_historical_data.jobs import extract_security
        from shared.hooks.data_lake import data_lake_hooks

        data_lake_hooks.checkout(
            checkout_path=file_path,
            func=lambda data: extract_security(data),
        )

    @task
    def extract_security_ticker(file_path: str):
        from dags.security.eod_historical_data.jobs import extract_security_ticker
        from shared.hooks.data_lake import data_lake_hooks

        data_lake_hooks.checkout(
            checkout_path=file_path,
            func=lambda data: extract_security_ticker(data),
        )

    @task
    def extract_security_listing(file_path: str):
        from dags.security.eod_historical_data.jobs import extract_security_listing
        from shared.hooks.data_lake import data_lake_hooks

        data_lake_hooks.checkout(
            checkout_path=file_path,
            func=lambda data: extract_security_listing(data),
        )

    ingest_task = ingest(exchange_code)
    transform_task = transform(ingest_task)

    extract_security(transform_task)
    extract_security_ticker(transform_task)
    extract_security_listing(transform_task)


with DAG(
    dag_id="security",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["security"],
) as dag:
    extract_exchange_codes_task = extract_exchange_codes()
    manage_one_exchange.expand(exchange_code=extract_exchange_codes_task)
