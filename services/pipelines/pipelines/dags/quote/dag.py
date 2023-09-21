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
        from shared.utils.dag.xcom import set_xcom_value

        set_xcom_value(key="security", value=security)

        return data_lake_hooks.checkout(
            func=lambda: ingest(ticker=security["ticker"], exchange_code=security["exchange_code"]),
            commit_path=SecurityQuotePath.raw(source=ASSET_SOURCE, bin=security["ticker"], file_type="json"),
        )

    @task
    def transform(file_path: str):
        from dags.quote.eod_historical_data.jobs import ASSET_SOURCE, transform
        from dags.quote.path import SecurityQuotePath
        from shared.hooks.data_lake import data_lake_hooks

        from shared.utils.dag.xcom import get_xcom_value

        security = get_xcom_value(task_id="manage_one_security.ingest", key="security", return_type=SecurityExtractDict)

        return data_lake_hooks.checkout(
            checkout_path=file_path,
            func=lambda data: transform(data=data, security_listing_id=security["id"]),
            commit_path=SecurityQuotePath.processed(source=ASSET_SOURCE, bin=security["ticker"]),
        )

    @task
    def load(file_path: str):
        from dags.quote.eod_historical_data.jobs import load
        from shared.hooks.data_lake import data_lake_hooks
        from shared.clients.data_lake.azure.azure_data_lake import dl_client

        # import pandas as pd
        from io import BytesIO
        import polars as pl

        b = BytesIO(dl_client.download_file_into_memory(file_path=file_path))

        df = pl.read_parquet(b)

        load(df)

        # data_lake_hooks.checkout(checkout_path=file_path, func=lambda data: load(data))

    ingest_task = ingest(security)
    transform_task = transform(ingest_task)
    load(transform_task)


with DAG(
    dag_id="historical_quote",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["quote", "security"],
) as dag:
    extract_security_info_task = extract_security_info()
    manage_one_security.expand(security=extract_security_info_task)
