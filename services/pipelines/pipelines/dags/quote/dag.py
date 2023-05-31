# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from dags.quote.eod_historical_data.jobs import SecurityExtractDict


@task
def extract_security_info():
    from dags.quote.eod_historical_data.jobs import extract

    return extract()


@task_group
def manage_one_security(security: SecurityExtractDict):
    """
    Manages downloading and processing of one index.
    """

    @task
    def ingest(security: SecurityExtractDict):
        from dags.quote.eod_historical_data.jobs import ASSET_SOURCE, ingest
        from dags.quote.path import SecurityQuotePath
        from shared.hooks.data_lake import data_lake_hooks

        return data_lake_hooks.checkout(
            func=lambda: ingest(security=security),
            commit_path=SecurityQuotePath.raw(source=ASSET_SOURCE, bin=security["security_code"], file_type="json"),
        )

    @task
    def transform(file_path: str, security: SecurityExtractDict):
        from dags.quote.eod_historical_data.jobs import ASSET_SOURCE, transform
        from dags.quote.path import SecurityQuotePath
        from shared.hooks.data_lake import data_lake_hooks

        return data_lake_hooks.checkout(
            checkout_path=file_path,
            func=lambda data: transform(data=data, security=security),
            commit_path=SecurityQuotePath.processed(source=ASSET_SOURCE, bin=security["security_code"]),
        )

    @task
    def load_to_db(file_path: str):
        from dags.quote.eod_historical_data.jobs import load
        from shared.hooks.data_lake import data_lake_hooks

        data_lake_hooks.checkout(checkout_path=file_path, func=lambda data: load(data))

    ingest_task = ingest(security)
    transform_task = transform(ingest_task, security)
    load_to_db(transform_task)


with DAG(
    dag_id="historical_quote",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["quote", "security"],
) as dag:
    extract_security_info_task = extract_security_info()
    manage_one_security.expand(security=extract_security_info_task)
