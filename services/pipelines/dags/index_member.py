# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag
from airflow.utils.trigger_rule import TriggerRule
import pyarrow as pa

from typing import TypedDict

from shared.path import IndexMemberPath, SecurityPath, AdlsPath, LocalPath
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from utils.dag.xcom import XComGetter


class Index(TypedDict):
    exchange_code: str
    index_code: str


@task
def extract_index():
    import polars as pl
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook

    dt = DeltaTableHook(conn_id="azure_data_lake").read(path=SecurityPath.curated())

    securities = pl.from_arrow(
        dt.to_pyarrow_dataset(
            partitions=[
                ("type", "=", "index"),
            ]
        ).to_batches()
    )

    if isinstance(securities, pl.DataFrame):
        s = securities.select(
            [
                pl.col("code").alias("index_code"),
                pl.col("exchange_code"),
            ],
        ).to_dicts()
        return s


def transform_on_ingest(data: bytes, local_download_dir: LocalPath):
    import json
    import polars as pl

    index_data = json.loads(data)

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

    lf.sink_parquet(local_download_dir.to_pathlib() / f"{general.get('Code')}.parquet")


@task
def ingest(index: list):
    from custom.hooks.api.shared import BulkApiHook

    api = BulkApiHook(
        conn_id="eod_historical_data",
        response_format="json",
        transform=transform_on_ingest,
        adls_upload=BulkApiHook.AdlsUploadConfig(
            path=IndexMemberPath,
            conn_id="azure_data_lake",
            record_mapping={
                "index": "index_code",
            },
        ),
    )
    api.run(endpoint="fundamentals/${index_code}.${exchange_code}", items=index)

    # upload as arrow ds
    destination_path = api.upload_dataset(
        conn_id="azure_data_lake",
        dataset_format="parquet",
    )

    return destination_path.to_dict()


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    adls_conn_id="azure_data_lake",
    dataset_path=XComGetter.pull_with_template(task_id="ingest"),
    destination_path=IndexMemberPath.curated(),
    pyarrow_options={
        "schema": pa.schema(
            [
                pa.field("security_code", pa.string()),
                pa.field("security_name", pa.string()),
                pa.field("exchange_code", pa.string()),
                pa.field("country", pa.string()),
                pa.field("currency", pa.string()),
                pa.field("index_code", pa.string()),
                pa.field("index_name", pa.string()),
            ]
        )
    },
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
    ingest(extract_index_task) >> sink


dag_object = index_member()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
