import io

import assets.blocks as block
import assets.path as cloud_path
import polars as pl
from custom.hooks.sql.trino.hook import IndexListModel, TrinoWarehouseHook
from custom.tasks.hive import update_hive_partitions
from hooks.http.eod import EodHistoricalDataHook, IndexMemberModel
from lib.hooks.aws.s3.async_upload_from_url import S3BatchUploadFromUrlHook, UploadItem
from prefect import flow, task


@task(log_prints=True)
def get_index_codes():
    """Retrieve index codes using Trino."""

    engine = block.trino_iceberg_warehouse()

    return TrinoWarehouseHook(engine).indexes()


def transform_func(data: bytes | str, id: str) -> bytes | str | io.BytesIO | None:
    upload_buffer = io.BytesIO()
    validated_data = IndexMemberModel.model_validate(data)

    # print(f"Validated data for index {id}: {validated_data}")

    members = validated_data.Components.values()

    # print(f"Members for index {id}: {members}")

    if not members or len(members) == 0:
        return None

    print(f"Members for index {id}: {len(members)}")

    df = pl.DataFrame(list(members)).with_columns(
        Index_Code=pl.coalesce(
            pl.lit(
                validated_data.General.Code,
            ),
            pl.lit(id),
        ),
        Index_Name=pl.lit(validated_data.General.Name),
    )

    df.write_parquet(upload_buffer)
    upload_buffer.seek(0)

    return upload_buffer


@task(log_prints=True)
def ingest_member(indexes: IndexListModel):

    eod_api_key = block.eod_api_key()
    s3_credentials = block.aws_s3_lakehouse()
    ingest_ts = cloud_path.ingest_ts()

    upload_hook = S3BatchUploadFromUrlHook(s3_credentials=s3_credentials)
    upload_hook.transform_func = transform_func
    upload_hook.upload_from_url(
        items=[
            UploadItem(
                url=EodHistoricalDataHook(eod_api_key).url_index_members(index_symbol=index.code),
                bucket="lakehouse",
                key=cloud_path.exchange_index_member_ingest_path(ingest_ts=ingest_ts),
                id=index.code,
            )
            for index in indexes
        ]
    )


@flow(log_prints=True)
def index_member_flow():
    index_codes = get_index_codes()

    ingest_member(indexes=index_codes)

    update_hive_partitions(block_name=block.TRINO__HIVE_WAREHOUSE, table_name="index_member")


if __name__ == "__main__":

    index_member_flow()
