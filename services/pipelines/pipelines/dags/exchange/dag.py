# pyright: reportUnusedExpression=false
from datetime import datetime
from typing import TypedDict

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


class MergePaths(TypedDict):
    eod_historical_data: str
    market_stack: str
    iso_mic: str


@task
def ingest_eod_historical_data():
    from dags.exchange.eod_historical_data.jobs import ASSET_SOURCE, ingest
    from dags.exchange.path import ExchangePath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(func=ingest, commit_path=ExchangePath.raw(source=ASSET_SOURCE, file_type="json"))


@task
def transform_eod_historical_data(file_path: str):
    from dags.exchange.eod_historical_data.jobs import ASSET_SOURCE, transform
    from dags.exchange.path import ExchangePath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path,
        func=lambda data: transform(data),
        commit_path=ExchangePath.processed(source=ASSET_SOURCE),
    )


@task
def ingest_iso_mic():
    from dags.exchange.iso_mic.jobs import ASSET_SOURCE, ingest
    from dags.exchange.path import ExchangePath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        func=ingest,
        commit_path=ExchangePath.raw(source=ASSET_SOURCE, file_type="csv"),
    )


@task
def transform_iso_mic(file_path: str):
    from dags.exchange.iso_mic.jobs import ASSET_SOURCE, transform
    from dags.exchange.path import ExchangePath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path,
        func=transform,
        commit_path=ExchangePath.processed(source=ASSET_SOURCE),
    )


@task
def ingest_market_stack():
    from dags.exchange.market_stack.jobs import ASSET_SOURCE, ingest
    from dags.exchange.path import ExchangePath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        func=ingest,
        commit_path=ExchangePath.raw(source=ASSET_SOURCE, file_type="json"),
    )


@task
def transform_market_stack(file_path: str):
    from dags.exchange.market_stack.jobs import ASSET_SOURCE, transform
    from dags.exchange.path import ExchangePath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path,
        func=lambda data: transform(data),
        commit_path=ExchangePath.processed(source=ASSET_SOURCE),
    )


@task
def merge(file_paths: MergePaths):
    from dags.exchange.path import ExchangePath
    from dags.exchange.shared.jobs import merge
    from shared.hooks.data_lake import data_lake_hooks

    merged = merge(
        {
            "eod_historical_data": data_lake_hooks.download(file_paths["eod_historical_data"]).to_polars_df(),
            "iso_mic": data_lake_hooks.download(file_paths["iso_mic"]).to_polars_df(),
            "market_stack": data_lake_hooks.download(file_paths["market_stack"]).to_polars_df(),
        }
    )

    return data_lake_hooks.upload(path=ExchangePath.temp(), data=merged)


@task
def add_surr_keys(file_path: str):
    from dags.exchange.path import ExchangePath
    from dags.exchange.shared.jobs import add_surr_keys
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path, func=lambda data: add_surr_keys(data), commit_path=ExchangePath.temp()
    )


@task
def load_into_db(file_path: str):
    from dags.exchange.shared.jobs import load_into_db
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path, func=lambda data: load_into_db(data), commit_path="test.parquet"
    )


deactivate_current_records = PostgresOperator(
    postgres_conn_id="postgres_database",
    task_id="deactivate_current_records",
    sql="""
    --sql
    UPDATE exchange SET
        is_active=FALSE,
        active_until = now(),
        updated_at = now()
    ;
    """,
)


with DAG(
    dag_id="exchange",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["exchange"],
) as dag:
    ingest_iso_task = ingest_iso_mic()
    transform_iso_task = transform_iso_mic(ingest_iso_task)

    ingest_eod_historical_data_task = ingest_eod_historical_data()
    transform_eod_historical_data_task = transform_eod_historical_data(ingest_eod_historical_data_task)

    ingest_market_stack_task = ingest_market_stack()
    transform_market_stack_task = transform_market_stack(ingest_market_stack_task)

    merged = merge(
        {
            "market_stack": transform_market_stack_task,
            "eod_historical_data": transform_eod_historical_data_task,
            "iso_mic": transform_iso_task,
        }
    )

    add_surr_keys_task = add_surr_keys(merged)

    merged >> add_surr_keys_task >> deactivate_current_records >> load_into_db(add_surr_keys_task)
