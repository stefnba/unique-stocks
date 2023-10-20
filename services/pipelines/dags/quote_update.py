# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
import logging
from datetime import datetime, timedelta
from typing import TypedDict

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from custom.operators.data.transformation import DataBindingCustomHandler, DuckDbTransformationOperator
from custom.providers.azure.hooks import converters
from custom.providers.azure.hooks.handlers.read.azure import AzureDatasetArrowHandler
from shared import airflow_dataset, schema
from shared.path import AdlsPath, LocalPath, SecurityQuotePath
from utils.dag.xcom import XComGetter

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


class QuoteSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def extract_exchange():
    from datetime import datetime

    import polars as pl
    from custom.hooks.data.mapping import MappingDatasetHook
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook
    from shared.path import SecurityQuotePath

    mapping = MappingDatasetHook().mapping(product="exchange", source="EodHistoricalData", field="composite_code")

    today = datetime.now()
    last_quote_per_exchange = (
        pl.scan_pyarrow_dataset(
            DeltaTableHook(conn_id="azure_data_lake").read(path=SecurityQuotePath.curated()).to_pyarrow_dataset()
        )
        .group_by("exchange_code")
        .agg([pl.max("date").str.to_date().alias("last_date")])
        .with_columns(today=pl.date(today.year, today.month, today.day))
        .with_columns(dates=pl.date_ranges(start="last_date", end="today", interval="1d", closed="none"))
        .select("exchange_code", pl.col("dates").cast(pl.List(pl.Utf8)))
    )

    exchange = last_quote_per_exchange.join(
        mapping.lazy().select("source_value", "mapping_value"),
        left_on="exchange_code",
        right_on="mapping_value",
        how="left",
    ).select(
        [
            pl.coalesce(["source_value", "exchange_code"]).alias("exchange_code"),
            "dates",
        ]
    )

    return exchange.collect().to_dicts()


def convert_to_url_upload_records(exchange: str, date: str):
    """Convert exchange code and date code a `UrlUploadRecord` with blob name and API endpoint."""
    from custom.providers.azure.hooks.data_lake_storage import UrlUploadRecord

    return UrlUploadRecord(
        endpoint=exchange,
        blob=SecurityQuotePath.raw(exchange=exchange, format="csv", security="BULK", source="EodHistoricalData").path,
        param={"date": date},
    )


@task()
def ingest(exchange):
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook
    from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook

    exchange_code = exchange.get("exchange_code")
    dates = exchange.get("dates", [])

    logging.info(f"""Ingest of quotes for exchange '{exchange_code}' for dates {dates}.""")

    hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
    api_hook = EodHistoricalDataApiHook()

    url_endpoints = list(map(lambda d: convert_to_url_upload_records(exchange=exchange_code, date=d), dates))

    uploaded_blobs = hook.upload_from_url(
        container="raw",
        base_url="https://eodhistoricaldata.com/api/eod-bulk-last-day",
        base_params=api_hook._base_params,
        url_endpoints=url_endpoints,
    )

    return uploaded_blobs


@task
def merge(blobs):
    import pyarrow as pa
    import pyarrow.dataset as ds
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook
    from custom.providers.azure.hooks.dataset import AzureDatasetHook
    from custom.providers.azure.hooks.handlers.write.azure import AzureDatasetWriteArrowHandler

    hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
    dataset_hook = AzureDatasetHook(conn_id="azure_data_lake")
    download_dir = LocalPath.create_temp_dir_path()
    adls_destination_path = AdlsPath.create_temp_dir_path()

    # flatten blobs
    blobs = [t for b in blobs for t in b]

    hook.download_blobs_from_list(
        container="raw",
        blobs=blobs,
        destination_dir=download_dir.uri,
        destination_nested_relative_to="security_quote/EodHistoricalData",
    )

    data = ds.dataset(
        source=download_dir.uri,
        format="csv",
        schema=pa.schema(
            [
                ("Date", pa.date32()),
                ("Open", pa.float32()),
                ("High", pa.float32()),
                ("Low", pa.float32()),
                ("Close", pa.float32()),
                ("Adjusted_close", pa.float32()),
                ("Volume", pa.float32()),
                ("Code", pa.string()),
                ("exchange", pa.string()),
            ]
        ),
        partitioning=ds.partitioning(
            schema=pa.schema(
                [
                    ("exchange", pa.string()),
                ]
            ),
            flavor="hive",
        ),
    )

    dataset_hook.write(dataset=data, destination_path=adls_destination_path, handler=AzureDatasetWriteArrowHandler)

    return adls_destination_path.to_dict()


transform = DuckDbTransformationOperator(
    task_id="transform",
    adls_conn_id="azure_data_lake",
    destination_path=AdlsPath.create_temp_file_path(),
    query="sql/quote_update/transform.sql",
    data={
        "quotes_raw": DataBindingCustomHandler(
            path=XComGetter.pull_with_template(task_id="merge"),
            handler=AzureDatasetArrowHandler,
            dataset_converter=converters.PyArrowDataset,
        )
    },
)

sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    adls_conn_id="azure_data_lake",
    dataset_path=XComGetter.pull_with_template(task_id="transform"),
    destination_path=SecurityQuotePath.curated(),
    pyarrow_options={
        "schema": schema.SecurityQuote,
    },
    delta_table_options={
        "mode": "append",
        "schema": schema.SecurityQuote,
    },
    outlets=[airflow_dataset.SecurityQuote],
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["quote", "security"],
    default_args=default_args,
)
def quote_update():
    extract_exchange_task = extract_exchange()
    ingest_task = ingest.expand(exchange=extract_exchange_task)

    merge(ingest_task) >> transform >> sink


dag_object = quote_update()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(
        conn_file_path=connections,
        run_conf={
            "delta_table_mode": "overwrite",
            "exchanges": ["XETRA", "NASDAQ"],
            "security_types": [
                "common_stock",
            ],
        },
    )
