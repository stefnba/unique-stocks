import io

import assets.blocks as block
import assets.path as cloud_path
import polars as pl
from custom.tasks.hive import update_hive_partitions
from hooks.http.eod import EodHistoricalDataHook, HistoricalRatesListModel
from lib.hooks.aws.s3.async_upload_from_url import S3BatchUploadFromUrlHook, UploadItem
from lib.hooks.sql.hook import SQLHook
from prefect import flow, task
from pydantic import BaseModel, RootModel


class ExchangeSecurity(BaseModel):
    code: str
    exchange_code: str
    exchange_code_api: str


class ExchangeSecurityList(RootModel):
    root: list[ExchangeSecurity]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]


@task(log_prints=True)
def get_securities() -> ExchangeSecurityList:
    """Retrieve exchange codes using Trino."""

    engine = block.trino_iceberg_warehouse()
    securities = SQLHook(engine).query_data(
        """
        SELECT
            s.code,
            s.exchange_code,
            COALESCE(e.composite_code, s.exchange_code) AS exchange_code_api
        FROM iceberg_warehouse.analytics.anlytcs__exchange_security s
        LEFT JOIN iceberg_warehouse.analytics.anlytcs__exchange e ON e.code = s.exchange_code
        WHERE exchange_code = 'INDX' 
        --AND type = 'Common Stock'
        LIMIT 1200
        """,
        data_model=ExchangeSecurityList,
    )

    return securities


def transform_func(data: bytes | str, id: str) -> bytes | str | io.BytesIO:
    upload_buffer = io.BytesIO()
    validated_data = HistoricalRatesListModel.model_validate(data)

    # return validated_data.model_dump_json()

    df = pl.DataFrame(
        validated_data.model_dump(),
        schema={
            "date": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "adjusted_close": pl.Float64,
            "volume": pl.Int64,
            "ingested_at": pl.Datetime,
            "exchange_code": pl.Utf8,
            "security_code": pl.Utf8,
        },
    )

    df.write_parquet(upload_buffer, use_pyarrow=True)
    upload_buffer.seek(0)

    return upload_buffer


@task
def ingest_quotes(securities: ExchangeSecurityList):
    aws_credentials = block.aws_s3_lakehouse()
    eod_api_key = block.eod_api_key()

    ingest_ts = cloud_path.ingest_ts()

    upload_hook = S3BatchUploadFromUrlHook(s3_credentials=aws_credentials)
    upload_hook.transform_func = transform_func
    upload_hook.upload_from_url(
        items=[
            UploadItem(
                url=EodHistoricalDataHook(eod_api_key).url_historical_rates(
                    symbol=security.code, exchange_code=security.exchange_code_api
                ),
                bucket="lakehouse",
                key=cloud_path.security_quote_ingest_path(
                    exchange_code=security.exchange_code, security_code=security.code, ingest_ts=ingest_ts
                ),
                id=f"{security.code}-{security.exchange_code}",
            )
            for security in securities
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
    securities = get_securities()

    ingest_quotes(securities)

    update_hive_partitions(block_name=block.TRINO__HIVE_WAREHOUSE, table_name="security_quote")

    trigger_dbt()


if __name__ == "__main__":

    exchange_security_flow()
