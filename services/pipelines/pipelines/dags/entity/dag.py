# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator


@task
def download_gleif_isin():
    """
    Download file into local file system.
    """
    from shared.clients.api.gleif.client import GleifApiClient
    from shared.utils.path.container.file_path import container_file_path

    file_path = container_file_path(format="zip")
    GleifApiClient.get_entity_isin_mapping(file_path=file_path)
    return file_path


@task
def download_gleif():
    """
    Download file into local file system.
    """
    from shared.clients.api.gleif.client import GleifApiClient
    from shared.utils.path.container.file_path import container_file_path

    file_path = container_file_path(format="zip")
    GleifApiClient.get_entity_list(file_path=file_path)
    return file_path


@task
def ingest_gleif(file_path: str):
    """
    Upload zipped file into data lake.
    """
    from shared.clients.data_lake.azure.azure_data_lake import dl_client
    from dags.entity.path import EntityPath
    from dags.entity.gleif.jobs import ASSET_SOURCE

    upload = dl_client.upload_file(
        file=file_path,
        stream=True,
        destination_file_path=EntityPath.raw(
            source=ASSET_SOURCE,
            file_type="zip",
        ),
    )

    return upload.file.full_path


@task
def ingest_gleif_isin(file_path: str):
    """
    Upload zipped file into data lake.
    """
    from shared.clients.data_lake.azure.azure_data_lake import dl_client
    from dags.entity.path import EntityIsinPath
    from dags.entity.gleif.jobs_entity_isin import ASSET_SOURCE

    remote_path = dl_client.upload_file(
        file=file_path,
        stream=True,
        destination_file_path=EntityIsinPath.raw(
            source=ASSET_SOURCE,
            file_type="zip",
        ),
    )

    return remote_path.file.full_path


@task
def unzip_convert_gleif():
    """
    Unpack zip file, convert to parquet file and then remove unziped file.
    """
    from dags.entity.gleif import jobs_shared
    from shared.utils.dag.xcom import get_xcom_value

    file_path = get_xcom_value(task_id="download_gleif", return_type=str)

    parquet_path = jobs_shared.unzip_convert(file_path=file_path)
    return parquet_path


@task
def unzip_convert_gleif_isin():
    """
    Unpack zip file, convert to parquet file and then remove unziped file.
    """
    from dags.entity.gleif import jobs_shared
    from shared.utils.dag.xcom import get_xcom_value

    file_path = get_xcom_value(task_id="download_gleif_isin", return_type=str)

    parquet_path = jobs_shared.unzip_convert(file_path=file_path)
    return parquet_path


@task
def transform_gleif(file_path: str):
    from dags.entity.gleif.jobs import transform, ASSET_SOURCE
    from dags.entity.path import EntityPath
    from shared.clients.data_lake.azure.azure_data_lake import dl_client
    import polars as pl

    df = pl.read_parquet(file_path)
    transform(df).write_parquet(file_path)

    upload = dl_client.upload_file(
        file=file_path, destination_file_path=EntityPath.processed(source=ASSET_SOURCE), stream=True
    )

    return upload.file.full_path


@task
def transform_gleif_isin(file_path: str):
    from dags.entity.gleif.jobs_entity_isin import transform, ASSET_SOURCE
    from dags.entity.path import EntityIsinPath
    from shared.clients.data_lake.azure.azure_data_lake import dl_client
    import polars as pl

    df = pl.read_parquet(file_path)
    transform(df).write_parquet(file_path)

    upload = dl_client.upload_file(
        file=file_path, destination_file_path=EntityIsinPath.processed(source=ASSET_SOURCE), stream=True
    )

    return upload.file.full_path


# @task
# def load_into_db(file_path: str):
#     from dags.entity.gleif.jobs import load_into_db
#     from shared.hooks.data_lake import data_lake_hooks

#     return data_lake_hooks.checkout(checkout_path=file_path, func=lambda data: load_into_db(data))


# deactivate_current_records = PostgresOperator(
#     postgres_conn_id="postgres_database",
#     task_id="deactivate_current_records",
#     sql="""
#     --sql
#     UPDATE "data"."entity" SET
#         is_active=FALSE,
#         active_until = now(),
#         updated_at = now()
#     ;
#     """,
# )


with DAG(
    dag_id="entity",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["entity"],
) as dag:
    download_gleif_task = download_gleif()
    download_gleif_isin_task = download_gleif_isin()

    ingest_gleif_task = ingest_gleif(download_gleif_task)
    ingest_gleif_isin_task = ingest_gleif_isin(download_gleif_isin_task)

    unzip_convert_gleif_task = unzip_convert_gleif()
    unzip_convert_gleif_isin_task = unzip_convert_gleif_isin()

    ingest_gleif_task >> unzip_convert_gleif_task
    ingest_gleif_isin_task >> unzip_convert_gleif_isin_task

    transform_gleif_task = transform_gleif(unzip_convert_gleif_task)
    transform_gleif_isin_task = transform_gleif_isin(unzip_convert_gleif_isin_task)
