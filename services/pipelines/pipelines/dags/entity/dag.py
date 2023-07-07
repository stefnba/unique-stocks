# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


@task
def ingest_gleif():
    from dags.entity.gleif.jobs import ingest, ASSET_SOURCE
    from dags.entity.path import EntityPath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(func=ingest, commit_path=EntityPath.raw(source=ASSET_SOURCE, file_type="zip"))


@task
def unzip_gleif(file_path: str):
    from shared.hooks.data_lake import data_lake_hooks
    from dags.entity.path import EntityPath
    from dags.entity.gleif import jobs

    from shared.clients.data_lake.azure.azure_data_lake import dl_client

    file = dl_client.download_file_into_memory(file_path)

    df = jobs.unzip(file)
    return data_lake_hooks.upload(df, EntityPath.temp(), format="parquet")


@task
def transform_gleif(file_path: str):
    from dags.entity.gleif.jobs import transform, ASSET_SOURCE
    from dags.entity.path import EntityPath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path,
        func=lambda data: transform(data),
        commit_path=EntityPath.processed(source=ASSET_SOURCE),
    )


@task
def add_surrogate_key(file_path: str):
    from dags.entity.path import EntityPath
    from dags.entity.gleif import jobs
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path, func=lambda data: jobs.add_surrogate_key(data), commit_path=EntityPath.temp()
    )


@task
def load_into_db(file_path: str):
    from dags.entity.gleif.jobs import load_into_db
    from shared.hooks.data_lake import data_lake_hooks

    data_lake_hooks.checkout(checkout_path=file_path, func=lambda data: load_into_db(data))


deactivate_current_records = PostgresOperator(
    postgres_conn_id="postgres_database",
    task_id="deactivate_current_records",
    sql="""
    --sql
    UPDATE "data"."entity" SET
        is_active=FALSE,
        active_until = now(),
        updated_at = now()
    ;
    """,
)


with DAG(
    dag_id="entity",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["entity"],
) as dag:
    ingest_gleif_task = ingest_gleif()
    unzip_gleif_task = unzip_gleif(ingest_gleif_task)
    transform_gleif_task = transform_gleif(unzip_gleif_task)

    add_surrogate_key(transform_gleif_task) >> deactivate_current_records >> load_into_db(transform_gleif_task)
