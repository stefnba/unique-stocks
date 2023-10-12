# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag
from string import Template

from typing import TypedDict

from utils.dag.xcom import XComGetter
from custom.providers.eod_historical_data.transformers.fundamental.common_stock import (
    EoDCommonStockFundamentalTransformer,
)
from shared.path import FundamentalPath, SecurityPath, LocalPath
from custom.hooks.api.shared import BulkApiHook
import pyarrow as pa
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator


class FundamentalSecurity(TypedDict):
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
        # "LSE",
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
        # .head(10)
        .collect().to_dicts()
    )


def transform_on_ingest(data: bytes, record: dict[str, str], local_download_dir: LocalPath):
    import json
    from custom.providers.eod_historical_data.transformers.utils import deep_get

    data_dict = json.loads(data)
    sec_type = deep_get(data_dict, ["General", "Type"])

    # Common stocks
    if sec_type == "Common Stock":
        # Only primary ticker
        if deep_get(data_dict, ["General", "PrimaryTicker"]) == Template(
            "${entity_code}.${api_exchange_code}"
        ).safe_substitute(record):
            transformer = EoDCommonStockFundamentalTransformer(data=data)
            transformed_data = transformer.transform()

            if transformed_data is not None:
                transformed_data.write_parquet(local_download_dir.to_pathlib() / f"{transformer.security}.parquet")

        return


def convert_to_url_upload_records(security: dict):
    """Convert exchange and security code into a `UrlUploadRecord` with blob name and API endpoint."""
    from custom.providers.azure.hooks.data_lake_storage import UrlUploadRecord

    exchange_code = security["exchange_code"]
    security_code = security["security_code"]
    api_exchange_code = security["api_exchange_code"]

    return UrlUploadRecord(
        endpoint=f"{security_code}.{api_exchange_code}",
        blob=FundamentalPath.raw(
            exchange=exchange_code,
            format="json",
            entity=security_code,
            source="EodHistoricalData",
        ).path,
    )


@task
def ingest(securities: list[dict[str, str]]):
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
        base_url="https://eodhistoricaldata.com/api/fundamentals",
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

    download_dir = LocalPath.create_temp_dir_path()

    hook.download_blobs_from_list(
        container="raw",
        blobs=blobs,
        destination_dir=download_dir.uri,
    )


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    adls_conn_id="azure_data_lake",
    dataset_path=XComGetter.pull_with_template(task_id="ingest"),
    destination_path=FundamentalPath.curated(),
    pyarrow_options={
        "schema": pa.schema(
            [
                pa.field("exchange_code", pa.string()),
                pa.field("security_code", pa.string()),
                pa.field("security_type", pa.string()),
                pa.field("category", pa.string()),
                pa.field("metric", pa.string()),
                pa.field("value", pa.string()),
                pa.field("currency", pa.string()),
                pa.field("period", pa.date32()),
                pa.field("period_type", pa.string()),
                pa.field("published_at", pa.date32()),
            ]
        )
    },
    delta_table_options={
        "mode": "overwrite",
        # "partition_by": [
        #     "security_type",
        #     "exchange_code",
        #     "security_code",
        # ],
    },
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["fundamental"],
)
def fundamental():
    extract_security_task = extract_security()
    ingest_task = ingest(extract_security_task)
    merge(ingest_task)
    # >> sink


dag_object = fundamental()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
