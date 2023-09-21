# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task


@task
def test(file_path: str):
    from shared.hooks.data_lake import data_lake_hooks

    from shared.clients.db.postgres.repositories import DbQueryRepositories

    return data_lake_hooks.checkout(
        checkout_path=file_path, func=lambda data: DbQueryRepositories.entity.bulk_add(data)
    )


with DAG(
    dag_id="test", schedule=None, start_date=datetime(2023, 1, 1), catchup=False, tags=["entity"], concurrency=1
) as dag:
    test(
        "/zone=processed/product=entity/source=GlobalLegalEntityIdentifierFoundation/year=2023/month=07/day=18/ts=20230718_090311____product=entity__source=GlobalLegalEntityIdentifierFoundation__zone=processed.parquet"
    )
