# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag, task_group
from airflow.utils.trigger_rule import TriggerRule
import pyarrow as pa

from typing import TypedDict
from custom.providers.eod_historical_data.operators.index_member.transform import EodIndexMemberTransformOperator

from shared.data_lake_path import IndexMemberPath, TempDirectory
from utils.dag.xcom import set_xcom_value, get_xcom_template
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator


class Index(TypedDict):
    exchange_code: str
    index_code: str


@task
def extract_index():
    import polars as pl
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook

    set_xcom_value(key="temp_dir", value=TempDirectory().directory)

    dt = DeltaTableHook(conn_id="azure_data_lake").read(source_path="security", source_container="curated")

    securities = pl.from_arrow(
        dt.to_pyarrow_dataset(
            partitions=[
                ("type", "=", "index"),
            ]
        ).to_batches()
    )

    if isinstance(securities, pl.DataFrame):
        s = securities.select([pl.col("code").alias("index_code"), pl.col("exchange_code")]).to_dicts()
        return s

    # return [
    #     # {"index_code": "GDAXI", "exchange_code": "INDX"},
    #     # {"index_code": "000906", "exchange_code": "INDX"},
    #     {"index_code": "5QQ0", "exchange_code": "INDX"},
    # ]


@task_group
def one_index(one_index: Index):
    @task()
    def ingest(index: Index):
        from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
        import json

        data = EodHistoricalDataApiHook().fundamental(
            security_code=index["index_code"], exchange_code=index["exchange_code"]
        )

        destination = IndexMemberPath.raw(source="EodHistoricalData", format="json").add_element(
            index=index["index_code"], exchange=index["exchange_code"]
        )
        WasbHook(wasb_conn_id="azure_blob").upload(
            data=json.dumps(data),
            container_name=destination.container,
            blob_name=destination.path,
        )

        set_xcom_value("exchange_code", index["exchange_code"])
        set_xcom_value("index_code", index["index_code"])

        return destination.serialized

    transform = EodIndexMemberTransformOperator(
        task_id="transform",
        adls_conn_id="azure_data_lake",
        destination_path=TempDirectory(
            base_dir=get_xcom_template(task_id="extract_index", key="temp_dir", use_map_index=False)
        ),
        source_path=get_xcom_template(task_id="one_index.ingest"),
    )

    ingest_task = ingest(one_index)
    ingest_task >> transform


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    trigger_rule=TriggerRule.ALL_DONE,
    adls_conn_id="azure_data_lake",
    dataset_path={"container": "temp", "path": get_xcom_template(task_id="extract_index", key="temp_dir")},
    destination_path=IndexMemberPath.curated(),
    pyarrow_options={
        "schema": pa.schema(
            [
                pa.field("code", pa.string()),
                pa.field("exchange_code", pa.string()),
                pa.field("name", pa.string()),
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
    one_index.expand(one_index=extract_index()) >> sink


dag_object = index_member()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
