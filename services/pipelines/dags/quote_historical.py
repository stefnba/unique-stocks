# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag


from typing import TypedDict


from shared.path import SecurityQuotePath, SecurityPath, AdlsPath, LocalPath
from utils.dag.xcom import XComGetter
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from custom.operators.data.transformation import DuckDbTransformationOperator, DataBindingCustomHandler
from custom.providers.azure.hooks.handlers.read.azure import AzureDatasetArrowHandler

from custom.providers.azure.hooks import converters
from shared import schema


class QuoteSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def extract_security():
    import polars as pl
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook
    from custom.hooks.data.mapping import MappingDatasetHook

    mapping = MappingDatasetHook().mapping(product="exchange", source="EodHistoricalData", field="composite_code")

    dt = DeltaTableHook(conn_id="azure_data_lake").read(path=SecurityPath.curated())

    exchanges = [
        "XETRA",
        "LSE",
        # "NASDAQ",
        # "NYSE",
    ]
    security_types = [
        "common_stock",
    ]

    securities = pl.scan_pyarrow_dataset(
        dt.to_pyarrow_dataset(
            partitions=[
                (
                    "exchange_code",
                    "in",
                    exchanges,
                ),
                ("type", "in", security_types),
            ]
        )
    )

    securities = securities.join(
        mapping.lazy().select(["source_value", "mapping_value"]),
        left_on="exchange_code",
        right_on="source_value",
        how="left",
    ).with_columns(pl.coalesce(["mapping_value", "exchange_code"]).alias("api_exchange_code"))

    return (
        securities.select(
            [
                pl.col("exchange_code"),
                "api_exchange_code",
                pl.col("code").alias("security_code"),
            ]
        )
        .head(10_000)
        .collect()
        .to_dicts()
    )


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


@task
def ingest(securities):
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook
    from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook

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

    hook.download_blobs_from_list(
        container="raw",
        blobs=blobs,
        destination_dir=download_dir.uri,
        destination_nested_relative_to="security_quote/EodHistoricalData",
    )

    data = ds.dataset(
        source=download_dir.uri,
        format="csv",
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
    query="sql/historical_quote/transform.sql",
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
        "mode": "overwrite",
        "schema": schema.SecurityQuote,
    },
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["quote", "security"],
)
def historical_quote():
    extract_security_task = extract_security()
    ingest_task = ingest(extract_security_task)

    merge(ingest_task) >> transform >> sink


dag_object = historical_quote()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
