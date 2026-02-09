import io

import assets.blocks as block
import assets.path as cloud_path
import polars as pl
from custom.hooks.sql.trino.hook import ExchangeListModel, TrinoWarehouseHook
from custom.tasks.hive import update_hive_partitions
from hooks.http.eod import EodHistoricalDataHook, ExchangeSecurityListModel
from lib.hooks.aws.s3.async_upload_from_url import S3BatchUploadFromUrlHook, UploadItem
from prefect import flow, task


@task(log_prints=True)
def get_exchange_code():
    """Retrieve exchange codes using Trino."""

    engine = block.trino_iceberg_warehouse()

    return TrinoWarehouseHook(engine).exchanges()


def transform_func(data: bytes | str, id: str) -> bytes | str | io.BytesIO:
    upload_buffer = io.BytesIO()
    validated_data = ExchangeSecurityListModel.model_validate(data)

    df = pl.DataFrame(validated_data.model_dump())

    df.write_parquet(upload_buffer)
    upload_buffer.seek(0)

    return upload_buffer


@task(log_prints=True)
def ingest_securities(exchanges: ExchangeListModel):

    eod_api_key = block.eod_api_key()
    s3_credentials = block.aws_s3_lakehouse()
    ingest_ts = cloud_path.ingest_ts()

    upload_hook = S3BatchUploadFromUrlHook(s3_credentials=s3_credentials)
    upload_hook.transform_func = transform_func
    upload_hook.upload_from_url(
        items=[
            UploadItem(
                url=EodHistoricalDataHook(eod_api_key).url_exchange_listed_securities(exchange_code=exchange.code),
                bucket="lakehouse",
                key=cloud_path.exchange_security_ingest_path(exchange_code=exchange.code, ingest_ts=ingest_ts),
                id=exchange.code,
            )
            for exchange in exchanges
        ]
    )


@task
def trigger_dbt():
    from lib.hooks.dbt.hook import DbtCoreHook

    DbtCoreHook(
        project_dir="../dbt/unique_stocks",
        profiles_dir="../dbt/unique_stocks",
    ).run(models="+anlytcs__security_quote")


@flow(log_prints=True)
def exchange_security_flow():
    exchanges = get_exchange_code()

    ingest_securities(exchanges=exchanges)

    update_hive_partitions(block_name=block.TRINO__HIVE_WAREHOUSE, table_name="exchange_security")

    trigger_dbt()


if __name__ == "__main__":

    exchange_security_flow()
