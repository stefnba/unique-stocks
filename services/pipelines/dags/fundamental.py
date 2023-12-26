# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
import logging
from datetime import datetime, timedelta
from typing import TypedDict

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from custom.providers.eod_historical_data.transformers.fundamental.common_stock import (
    EoDCommonStockFundamentalTransformer,
)
from shared import schema
from shared.path import FundamentalPath, LocalPath, SecurityPath
from utils.dag.xcom import XComGetter
from utils.filesystem.directory import DirFile

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


class FundamentalSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def extract_security():
    import polars as pl
    from custom.hooks.data.mapping import MappingDatasetHook
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook
    from utils.dag.conf import get_dag_conf

    conf = get_dag_conf()
    exchanges = conf.get("exchanges", [])
    security_types = conf.get("security_types", [])

    # Filter out indexes since we get their fundamentals, i.e. members, with different DAG
    exchanges = [e for e in exchanges if e != "INDX"]
    security_types = [e for e in security_types if e != "index"]

    logging.info(
        f"""Getting fundamentals for exchanges: '{", ".join(exchanges)}' and security types: '{", ".join(security_types)}'."""
    )

    mapping = MappingDatasetHook().mapping(product="exchange", source="EodHistoricalData", field="composite_code")

    data = (
        DeltaTableHook(conn_id="azure_data_lake")
        .read(path=SecurityPath.curated())
        .to_pyarrow_dataset(
            partitions=[
                ("exchange_code", "in", exchanges),
                ("type", "in", security_types),
            ]
        )
    )

    securities = pl.scan_pyarrow_dataset(data).select(pl.col("code").alias("security_code"), pl.col("exchange_code"))

    securities = securities.join(
        mapping.lazy().select(["source_value", "mapping_value"]),
        left_on="exchange_code",
        right_on="source_value",
        how="left",
    ).with_columns(pl.coalesce(["mapping_value", "exchange_code"]).alias("api_exchange_code"))

    return [
        securities.select(["exchange_code", "api_exchange_code", "security_code"])
        .collect()
        .filter(pl.col("exchange_code") == e)
        # .head(3)
        .to_dicts()
        for e in exchanges
    ]


def transform(file: DirFile, sink_dir: str):
    import json
    from pathlib import Path

    from custom.providers.eod_historical_data.transformers.utils import deep_get

    data = Path(file.path).read_text()

    data_dict = json.loads(data)
    sec_type = deep_get(data_dict, ["General", "Type"])

    transformed_data = None

    if sec_type == "Common Stock":
        transformer = EoDCommonStockFundamentalTransformer(data=data)
        transformed_data = transformer.transform()

    if transformed_data is not None:
        sink_path = Path(sink_dir) / LocalPath.create_temp_file_path(format="parquet").path
        transformed_data.write_parquet(sink_path)


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


@task(max_active_tis_per_dag=1)
def ingest(securities: list[dict[str, str]]):
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook
    from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook

    logging.info(f"""Ingest of securities for exchange '{securities[0].get("exchange_code", "")}'.""")

    hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
    api_hook = EodHistoricalDataApiHook()

    url_endpoints = list(map(convert_to_url_upload_records, securities))

    uploaded_blobs = hook.upload_from_url(
        container="raw",
        base_url="https://eodhistoricaldata.com/api/fundamentals",
        base_params=api_hook._base_params,
        url_endpoints=url_endpoints,
    )

    return uploaded_blobs


@task
def merge(blobs):
    import pyarrow.dataset as ds
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook
    from custom.providers.azure.hooks.dataset import AzureDatasetHook
    from custom.providers.azure.hooks.handlers.write.azure import AzureDatasetWriteArrowHandler
    from utils.filesystem.directory import scan_dir_files
    from utils.filesystem.path import AdlsPath, LocalPath
    from utils.parallel.concurrent import proces_paralell

    hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
    dataset_hook = AzureDatasetHook(conn_id="azure_data_lake")

    download_dir = LocalPath.create_temp_dir_path()
    sink_dir = LocalPath.create_temp_dir_path()
    adls_destination_path = AdlsPath.create_temp_dir_path()

    # flatten blobs
    blobs = [t for b in blobs for t in b]

    hook.download_blobs_from_list(
        container="raw",
        blobs=blobs,
        destination_dir=download_dir.uri,
    )

    files = scan_dir_files(download_dir.uri)

    proces_paralell(task=transform, iterable=files, sink_dir=sink_dir.uri)

    dataset = ds.dataset(source=sink_dir.uri)

    dataset_hook.write(dataset=dataset, destination_path=adls_destination_path, handler=AzureDatasetWriteArrowHandler)

    return adls_destination_path.to_dict()


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    adls_conn_id="azure_data_lake",
    dataset_path=XComGetter.pull_with_template(task_id="merge"),
    # destination_path=FundamentalPath.curated(),
    destination_path="temp/tesssst",
    pyarrow_options={
        "schema": schema.Fundamental,
    },
    delta_table_options={
        "mode": "{{ dag_run.conf.get('delta_table_mode', 'overwrite') }}",  # type: ignore
        "schema": schema.Fundamental,
        "partition_by": [
            "security_type",
            "exchange_code",
            "security_code",
        ],
    },
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["fundamental"],
    default_args=default_args,
)
def fundamental():
    extract_security_task = extract_security()
    ingest_task = ingest.expand(securities=extract_security_task)
    merge(ingest_task) >> sink


dag_object = fundamental()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(
        conn_file_path=connections,
        run_conf={
            "delta_table_mode": "overwrite",
            "exchanges": ["XETRA", "NASDAQ", "NYSE"],
            "security_types": [
                "common_stock",
            ],
        },
    )
