import io

import assets.blocks as block
import assets.path as cloud_path
import polars as pl
from custom.tasks.hive import update_hive_partitions
from hooks.http.eod import EodHistoricalDataHook
from prefect import flow, task
from prefect_aws.s3 import S3Bucket


@task(log_prints=True)
def ingest():

    aws_credentials = block.aws_s3_lakehouse()
    eod_api_key = block.eod_api_key()

    hook = EodHistoricalDataHook(api_token=eod_api_key)

    indexes = hook.indexes()

    s3_bucket = S3Bucket(bucket_name="lakehouse", credentials=aws_credentials)

    # Write exchanges to Parquet
    upload_buffer = io.BytesIO()
    indexes.to_polars(
        schema={
            "Code": pl.Utf8,
            "Name": pl.Utf8,
            "Country": pl.Utf8,
            "Currency": pl.Utf8,
            "Exchange": pl.Utf8,
            "Type": pl.Utf8,
            "Isin": pl.Utf8,
        }
    ).write_parquet(upload_buffer)
    upload_buffer.seek(0)

    # Upload exchanges JSON to S3
    s3_bucket_path = s3_bucket.upload_from_file_object(
        upload_buffer, cloud_path.exchange_security_ingest_path(exchange_code="INDX", ingest_ts=cloud_path.ingest_ts())
    )

    print(f"Uploaded file to {s3_bucket_path}")


@task
def trigger_dbt():
    from lib.hooks.dbt.hook import DbtCoreHook

    DbtCoreHook(
        project_dir="../dbt/unique_stocks",
        profiles_dir="../dbt/unique_stocks",
    ).run(models="+anlytcs__security_quote")


@flow(log_prints=True)
def index_flow():
    ingest()

    update_hive_partitions(block_name=block.TRINO__HIVE_WAREHOUSE, table_name="exchange_security")

    trigger_dbt()


if __name__ == "__main__":

    index_flow()
