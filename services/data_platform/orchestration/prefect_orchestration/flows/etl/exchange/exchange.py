import io

import assets.blocks as block
import assets.path as cloud_path
from custom.tasks.hive import update_hive_partitions
from hooks.http.eod import EodHistoricalDataHook
from prefect import flow, task
from prefect_aws.s3 import S3Bucket


@task(log_prints=True)
def ingest():
    """Ingest exchanges from EOD Historical Data API and upload to S3 as parquet file."""

    hook = EodHistoricalDataHook(api_token=block.eod_api_key())

    # get a list of exchanges
    exchanges = hook.exchanges()

    aws_credentials = block.aws_s3_lakehouse()

    s3_bucket = S3Bucket(bucket_name="lakehouse", credentials=aws_credentials)

    # Write exchanges to Parquet
    upload_buffer = io.BytesIO()
    exchanges.to_polars().write_parquet(upload_buffer)
    upload_buffer.seek(0)

    # Upload exchanges JSON to S3
    s3_bucket_path = s3_bucket.upload_from_file_object(upload_buffer, cloud_path.exchange_ingest_path())

    print(f"Uploaded file to {s3_bucket_path}")


@task
def trigger_dbt():
    from lib.hooks.dbt.hook import DbtCoreHook

    DbtCoreHook(
        project_dir="../dbt/unique_stocks",
        profiles_dir="../dbt/unique_stocks",
    ).run(models="+anlytcs__exchange")


@flow(log_prints=True)
def exchange_flow():

    exchanges = ingest()

    update_hive_partitions(block_name=block.TRINO__HIVE_WAREHOUSE, table_name="exchange")
    trigger_dbt()
    return exchanges


if __name__ == "__main__":
    exchange_flow()
