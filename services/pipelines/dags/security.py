# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime
from typing import TypedDict

import pyarrow as pa
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from custom.operators.data.transformation import DataBindingCustomHandler, DuckDbTransformationOperator
from custom.providers.azure.hooks import converters
from custom.providers.azure.hooks.handlers.read.azure import AzureDatasetArrowHandler
from shared import airflow_dataset
from shared.path import AdlsPath, ExchangePath, SecurityPath
from utils.dag.xcom import XComGetter


class Exchange(TypedDict):
    code: str


@task
def extract_exchange():
    import polars as pl
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook

    dt = DeltaTableHook(conn_id="azure_data_lake").read(path=ExchangePath.curated())

    exchanges = pl.from_arrow(dt.to_pyarrow_dataset().to_batches())

    if isinstance(exchanges, pl.DataFrame):
        return [
            *exchanges.select(
                [
                    pl.col("code").alias("exchange_code"),
                ]
            ).to_dicts(),
            {"exchange_code": "INDX"},  # add virtual exchange INDEX to get all index
        ]


def convert_to_url_upload_records(exchange: dict):
    """Convert exchange and security code into a `UrlUploadRecord` with blob name and API endpoint."""
    from custom.providers.azure.hooks.data_lake_storage import UrlUploadRecord
    from shared.path import SecurityPath

    exchange_code = exchange["exchange_code"]

    return UrlUploadRecord(
        endpoint=f"{exchange_code}",
        blob=SecurityPath.raw(
            exchange=exchange_code,
            format="csv",
            source="EodHistoricalData",
        ).path,
    )


@task
def ingest(exchanges: list):
    import logging

    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook
    from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook

    logging.info(f"""Ingest of securities of {len(exchanges)} exchanges.""")

    hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
    api_hook = EodHistoricalDataApiHook()

    url_endpoints = list(map(convert_to_url_upload_records, exchanges))

    uploaded_blobs = hook.upload_from_url(
        container="raw",
        base_url="https://eodhistoricaldata.com/api/exchange-symbol-list",
        base_params=api_hook._base_params,
        url_endpoints=url_endpoints,
    )

    return uploaded_blobs


@task
def merge(blobs):
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook
    from custom.providers.azure.hooks.dataset import AzureDatasetHook, DatasetConverters, DatasetHandlers
    from shared.path import AdlsPath, LocalPath

    storage_hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
    dataset_hook = AzureDatasetHook(conn_id="azure_data_lake")
    download_dir = LocalPath.create_temp_dir_path()
    adls_destination_path = AdlsPath.create_temp_dir_path()

    storage_hook.download_blobs_from_list(
        container="raw",
        blobs=blobs,
        destination_dir=download_dir.uri,
        # destination_nested_relative_to="security/EodHistoricalData",
    )

    data = dataset_hook.read(
        source_path=download_dir,
        handler=DatasetHandlers.Read.Local.Arrow,
        source_format="csv",
        dataset_converter=DatasetConverters.PyArrowDataset,
        schema=pa.schema(
            [
                pa.field("Code", pa.string()),
                pa.field("Name", pa.string()),
                pa.field("Country", pa.string()),
                pa.field("Exchange", pa.string()),
                pa.field("Currency", pa.string()),
                pa.field("Type", pa.string()),
                pa.field("Isin", pa.string()),
            ]
        ),
    )

    dataset_hook.write(dataset=data, handler=DatasetHandlers.Write.Azure.Arrow, destination_path=adls_destination_path)

    return adls_destination_path.to_dict()


transform = DuckDbTransformationOperator(
    task_id="transform",
    adls_conn_id="azure_data_lake",
    destination_path=AdlsPath.create_temp_file_path(),
    query="sql/security/transform.sql",
    data={
        "security_raw": DataBindingCustomHandler(
            path=XComGetter.pull_with_template(task_id="merge"),
            handler=AzureDatasetArrowHandler,
            dataset_converter=converters.PyArrowDataset,
        )
    },
)


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    adls_conn_id="azure_data_lake",
    dataset_path=XComGetter.pull_with_template(task_id="transform"),
    destination_path=SecurityPath.curated(),
    pyarrow_options={
        "schema": pa.schema(
            [
                pa.field("code", pa.string()),
                pa.field("name", pa.string()),
                pa.field("isin", pa.string()),
                pa.field("country", pa.string()),
                pa.field("currency", pa.string()),
                pa.field("exchange_code", pa.string()),
                pa.field("type", pa.string()),
            ]
        )
    },
    delta_table_options={
        "mode": "overwrite",
        "partition_by": ["exchange_code", "type"],
    },
    outlets=[airflow_dataset.Security],
)


@dag(
    schedule=[airflow_dataset.Exchange],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["security"],
)
def security():
    extract_exchange_task = extract_exchange()
    ingest_task = ingest(extract_exchange_task)
    merge(ingest_task) >> transform >> sink


dag_object = security()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
