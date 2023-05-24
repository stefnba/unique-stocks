# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task


@task
def ingest():
    from dags.index.eod_historical_data.jobs import ASSET_SOURCE, ingest
    from dags.index.path import IndexPath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(func=ingest, commit_path=IndexPath.raw(source=ASSET_SOURCE, file_type="json"))


@task
def transform(file_path: str):
    from dags.index.eod_historical_data.jobs import ASSET_SOURCE, transform
    from dags.index.path import IndexPath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path, func=lambda data: transform(data), commit_path=IndexPath.processed(source=ASSET_SOURCE)
    )


with DAG(
    dag_id="index",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["index"],
) as dag:
    ingest_task = ingest()
    transform(ingest_task)
