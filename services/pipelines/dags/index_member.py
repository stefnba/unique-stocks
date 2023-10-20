# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime
from typing import TypedDict

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from shared import schema
from shared.path import IndexMemberPath, SecurityPath
from utils.dag.xcom import XComGetter
from utils.filesystem.directory import DirFile


class Index(TypedDict):
    exchange_code: str
    index_code: str


@task
def extract_index():
    import polars as pl
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook

    dt = DeltaTableHook(conn_id="azure_data_lake").read(path=SecurityPath.curated())

    securities = pl.scan_pyarrow_dataset(dt.to_pyarrow_dataset(partitions=[("type", "=", "index")]))

    return (
        securities.select(
            [
                pl.col("code").alias("index_code"),
                pl.col("exchange_code"),
            ],
        )
        .collect()
        .to_dicts()
    )


def transform(file: DirFile, sink_dir: str):
    import json
    from pathlib import Path

    import polars as pl

    index_data = json.loads(Path(file.path).read_text())

    members = index_data.get("Components")
    general = index_data.get("General")

    if not members:
        return

    # transform
    lf = pl.LazyFrame([m for m in members.values()])

    lf = lf.select(
        [
            pl.col("Code").alias("security_code"),
            pl.col("Name").alias("security_name"),
            pl.col("Exchange").alias("exchange_code"),
        ]
    ).with_columns(
        [
            pl.lit(general.get("Code")).alias("index_code"),
            pl.lit(general.get("Name")).alias("index_name"),
            pl.lit(general.get("CurrencyCode")).alias("currency"),
            pl.lit(general.get("CountryISO")).alias("country"),
        ]
    )

    lf.sink_parquet(Path(sink_dir) / f"{general.get('Code')}.parquet")


@task
def ingest(index: list):
    import logging

    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook, UrlUploadRecord
    from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook
    from shared.path import IndexMemberPath

    logging.info(f"""Ingest of securities of {len(index)} exchanges.""")

    hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
    api_hook = EodHistoricalDataApiHook()

    url_endpoints = list(
        map(
            lambda i: UrlUploadRecord(
                endpoint=f"""{i["index_code"]}.{i["exchange_code"]}""",
                blob=IndexMemberPath.raw(
                    index=i["index_code"],
                    format="json",
                    source="EodHistoricalData",
                ).path,
            ),
            index,
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
    from custom.providers.azure.hooks.dataset import AzureDatasetHook, DatasetConverters, DatasetHandlers
    from shared.path import AdlsPath, LocalPath
    from utils.filesystem.directory import scan_dir_files
    from utils.parallel.concurrent import proces_paralell

    storage_hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
    dataset_hook = AzureDatasetHook(conn_id="azure_data_lake")
    sink_dir = LocalPath.create_temp_dir_path()
    download_dir = LocalPath.create_temp_dir_path()
    adls_destination_path = AdlsPath.create_temp_dir_path()

    storage_hook.download_blobs_from_list(
        container="raw",
        blobs=blobs,
        destination_dir=download_dir.uri,
    )

    files = scan_dir_files(download_dir.uri)

    proces_paralell(task=transform, iterable=files, sink_dir=sink_dir.uri)

    data = dataset_hook.read(
        source_path=sink_dir,
        handler=DatasetHandlers.Read.Local.Arrow,
        dataset_converter=DatasetConverters.PyArrowDataset,
        schema=schema.IndexMember,
    )

    dataset_hook.write(dataset=data, handler=DatasetHandlers.Write.Azure.Arrow, destination_path=adls_destination_path)

    return adls_destination_path.to_dict()


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    adls_conn_id="azure_data_lake",
    dataset_path=XComGetter.pull_with_template(task_id="merge"),
    destination_path=IndexMemberPath.curated(),
    pyarrow_options={"schema": schema.IndexMember},
    delta_table_options={"mode": "overwrite"},
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["index", "security"],
)
def index_member():
    extract_index_task = extract_index()
    ingest_task = ingest(extract_index_task)
    merge(ingest_task) >> sink


dag_object = index_member()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
