# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task, dag, task_group
import pyarrow as pa

from typing import TypedDict
from custom.operators.data.transformation import DuckDbTransformationOperator

from shared.data_lake_path import SecurityPath, TempDirectory
from utils.dag.xcom import XComGetter, set_xcom_value, get_xcom_template
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator


class Exchange(TypedDict):
    code: str


@task
def extract_exchange():
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook
    import polars as pl

    set_xcom_value(key="temp_dir", value=TempDirectory().directory)

    dt = DeltaTableHook(conn_id="azure_data_lake").read(source_path="exchange", source_container="curated")

    exchanges = pl.from_arrow(dt.to_pyarrow_dataset().to_batches())

    if isinstance(exchanges, pl.DataFrame):
        return exchanges.select("code").to_dicts()

    return [
        # {"code": "US"},
        # {"code": "XETRA"},
        {"code": "GSE"},
        {"code": "VFEX"},
        # {"code": "F"},
        # {"code": "FOREX"},
    ]


@task_group
def one_exchange(one_exchange: str):
    @task()
    def ingest(exchange: Exchange):
        from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
        import json

        data = EodHistoricalDataApiHook().exchange_security(exchange_code=exchange["code"])

        # some exchanges return empty response
        if len(data) == 0:
            return

        destination = SecurityPath.raw(source="EodHistoricalData", format="json").add_element(exchange=exchange["code"])
        WasbHook(wasb_conn_id="azure_blob").upload(
            data=json.dumps(data),
            container_name=destination.container,
            blob_name=destination.path,
        )

        set_xcom_value("exchange_code", exchange["code"])

        return destination.serialized

    transform = DuckDbTransformationOperator(
        task_id="transform",
        adls_conn_id="azure_data_lake",
        destination_path=TempDirectory(
            base_dir=get_xcom_template(task_id="extract_exchange", key="temp_dir", use_map_index=False)
        ),
        query="sql/security/transform.sql",
        data={"security_raw": get_xcom_template(task_id="one_exchange.ingest")},
        query_args={
            "exchange_code": XComGetter(task_id="one_exchange.ingest", key="exchange_code"),
        },
    )

    ingest_task = ingest(one_exchange)
    ingest_task >> transform


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    trigger_rule=TriggerRule.ALL_DONE,
    adls_conn_id="azure_data_lake",
    dataset_path={"container": "temp", "path": get_xcom_template(task_id="extract_exchange", key="temp_dir")},
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
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["security"],
)
def security():
    one_exchange.expand(one_exchange=extract_exchange()) >> sink


dag_object = security()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
