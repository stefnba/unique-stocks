# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime, timedelta

from airflow.decorators import task, dag
from airflow.models.param import Param

from typing import TypedDict


from shared.path import SecurityQuotePath, SecurityPath, AdlsPath, LocalPath
from utils.dag.xcom import XComGetter
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from custom.operators.data.transformation import DuckDbTransformationOperator, DataBindingCustomHandler
from custom.providers.azure.hooks.handlers.read.azure import AzureDatasetArrowHandler

from custom.providers.azure.hooks import converters
from shared import schema, airflow_dataset
from airflow.utils.dates import days_ago
import logging

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
def extract_security():
    import polars as pl
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook
    from custom.hooks.data.mapping import MappingDatasetHook
    from utils.dag.conf import get_dag_conf

    conf = get_dag_conf()

    mapping = MappingDatasetHook().mapping(product="exchange", source="EodHistoricalData", field="composite_code")

    exchanges = conf.get("exchanges", [])
    security_types = conf.get("security_types", [])

    logging.info(
        f"""Getting security quotes for exchanges: '{", ".join(exchanges)}' and security types: '{", ".join(security_types)}'."""
    )

    data = (
        DeltaTableHook(conn_id="azure_data_lake")
        .read(path=SecurityPath.curated())
        .to_pyarrow_dataset(
            partitions=[
                (
                    "exchange_code",
                    "in",
                    exchanges,
                ),
                ("type", "in", security_types),
            ],
        )
    )

    securities = pl.scan_pyarrow_dataset(data).select(pl.col("code").alias("security_code"), pl.col("exchange_code"))

    securities = securities.join(
        mapping.lazy().select(["source_value", "mapping_value"]),
        left_on="exchange_code",
        right_on="source_value",
        how="left",
    ).with_columns(
        pl.coalesce(["mapping_value", "exchange_code"]).alias("api_exchange_code"),
    )

    return [
        securities.select(["exchange_code", "api_exchange_code", "security_code"])
        .collect()
        .filter(pl.col("exchange_code") == e)
        .to_dicts()
        for e in exchanges
    ]


def convert_to_url_upload_records(security: dict):
    """Convert exchange and security code into a `UrlUploadRecord` with blob name and API endpoint."""
    from custom.providers.azure.hooks.data_lake_storage import UrlUploadRecord

    exchange_code = security["exchange_code"]
    security_code = security["security_code"]
    api_exchange_code = security["api_exchange_code"]

    return UrlUploadRecord(
        endpoint=f"{security_code}.{api_exchange_code}",
        blob=SecurityQuotePath.raw(
            exchange=exchange_code,
            format="csv",
            security=security_code,
            source="EodHistoricalData",
        ).path,
    )


@task(max_active_tis_per_dag=1)
def ingest(securities):
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook
    from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook

    logging.info(f"""Ingest of securities for exchange '{securities[0].get("exchange_code", "")}'.""")

    hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
    api_hook = EodHistoricalDataApiHook()

    url_endpoints = list(
        map(
            convert_to_url_upload_records,
            securities,
        )
    )

    uploaded_blobs = hook.upload_from_url(
        container="raw",
        base_url="https://eodhistoricaldata.com/api/eod",
        base_params=api_hook._base_params,
        url_endpoints=url_endpoints,
    )

    return uploaded_blobs


@task
def merge(blobs):
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook
    from custom.providers.azure.hooks.dataset import AzureDatasetHook
    from custom.providers.azure.hooks.handlers.write.azure import AzureDatasetWriteArrowHandler
    import pyarrow.dataset as ds
    import pyarrow as pa

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
                ("security", pa.string()),
                ("exchange", pa.string()),
            ]
        ),
        partitioning=ds.partitioning(
            schema=pa.schema(
                [
                    ("security", pa.string()),
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
    query="sql/quote_historical/transform.sql",
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
        "mode": "{{ dag_run.conf.get('delta_table_mode', 'overwrite') }}",  # type: ignore
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
    params={
        "exchanges": Param(
            type="array",
            default=[
                "XETRA",
                "NASDAQ",
                "NYSE",
                "INDX",
            ],
            examples=[
                "XETRA",
                "NASDAQ",
                "NYSE",
                "LSE",
                "SW",
            ],
        ),
        "security_types": Param(
            type="array",
            default=[
                "common_stock",
                "index",
                "preferred_stock",
            ],
            examples=[
                "common_stock",
                "index",
                "preferred_stock",
            ],
        ),
    },
)
def quote_historical():
    extract_security_task = extract_security()
    ingest_task = ingest.expand(securities=extract_security_task)

    merge(ingest_task) >> transform >> sink


dag_object = quote_historical()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(
        conn_file_path=connections,
        run_conf={
            "delta_table_mode": "overwrite",
            "exchanges": ["XETRA", "NASDAQ", "INDX"],
            "security_types": [
                "common_stock",
            ],
        },
    )
