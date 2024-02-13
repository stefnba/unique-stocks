# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime
from typing import TypedDict

from airflow.decorators import dag, task
from shared import airflow_dataset
from shared import connections as CONN
from shared.path import ADLSRawZonePath
from utils.filesystem.directory import DirFile


class Index(TypedDict):
    exchange_code: str
    index_code: str


@task
def extract_index():
    import polars as pl
    from custom.providers.iceberg.hooks.pyiceberg import IcebergStaticTableHook

    securities = IcebergStaticTableHook(
        io_conn_id=CONN.AWS_DATA_LAKE,
        catalog_conn_id=CONN.ICEBERG_CATALOG,
        catalog_name="uniquestocks",
        table_name=("curated", "security"),
    ).to_polars("type = 'index'")

    return securities.select(
        [
            pl.col("code").alias("index_code"),
            pl.col("exchange_code"),
        ],
    ).to_dicts()


@task
def set_sink_path():
    """
    Set sink directory on ADLS for ingestion.
    """

    sink_path = ADLSRawZonePath(product="index_member", source="EodHistoricalData", type="json")

    return f"{sink_path.path.scheme}://{sink_path.path.container}/{sink_path.path.dirs}"


@task
def map_url_sink_path(securities: list[dict]):
    """
    Take list of indexes and add endpoint and blob path to each record.
    """

    from airflow.exceptions import AirflowException
    from airflow.models.taskinstance import TaskInstance
    from airflow.operators.python import get_current_context
    from shared.path import ADLSRawZonePath

    context = get_current_context()

    ti = context.get("task_instance")

    if not isinstance(ti, TaskInstance):
        raise AirflowException("No task instance found.")

    path = ADLSRawZonePath.parse(path=ti.xcom_pull(task_ids="set_sink_path"), extension="json")

    return list(
        map(
            lambda exchange: {
                "endpoint": f"{exchange['index_code']}.{exchange['api_exchange_code']}",
                "blob": path.add_partition(
                    {"exchange": exchange["exchange_code"], "index": exchange["index_code"]}
                ).blob,
            },
            securities,
        )
    )


@task
def ingest(indexes):
    pass


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


@dag(
    schedule=[airflow_dataset.Security],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["index", "security"],
)
def index_member():
    extract_task = extract_index()
    ingest_task = ingest(extract_task)


dag_object = index_member()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
