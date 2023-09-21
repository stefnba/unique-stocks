# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag, task_group


from typing import TypedDict

from utils.dag.xcom import XComGetter, set_xcom_value, get_xcom_template
from custom.providers.eod_historical_data.operators.fundamental.transform import EodFundamentalTransformOperator
from shared.data_lake_path import FundamentalPath, TempDirectory
from airflow.utils.trigger_rule import TriggerRule
import pyarrow as pa
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator


class FundamentalSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def extract_security():
    import polars as pl
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook

    set_xcom_value(key="temp_dir", value=TempDirectory().directory)

    # return [{"security_code": "8TI", "exchange_code": "XETRA"}]

    dt = DeltaTableHook(conn_id="azure_data_lake").read(source_path="security", source_container="curated")

    securities = pl.from_arrow(
        dt.to_pyarrow_dataset(
            partitions=[
                (
                    "exchange_code",
                    "in",
                    [
                        "XETRA",
                        # "NASDAQ",
                        # "NYSE",
                    ],
                ),
                ("type", "=", "common_stock"),
            ]
        ).to_batches()
    )

    if isinstance(securities, pl.DataFrame):
        s = securities.select([pl.col("code").alias("security_code"), pl.col("exchange_code")]).to_dicts()

        return s[:1000]


@task_group
def one_fundamental(one_fundamental: FundamentalSecurity):
    @task
    def ingest(security: FundamentalSecurity):
        from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
        import json

        fundamental_data = EodHistoricalDataApiHook().fundamental(**security)

        destination = FundamentalPath.raw(source="EodHistoricalData", format="json").add_element(
            entity=security["security_code"]
        )

        WasbHook(wasb_conn_id="azure_blob").upload(
            data=json.dumps(fundamental_data),
            container_name=destination.container,
            blob_name=destination.path,
        )

        set_xcom_value(key="entity_id", value=security["security_code"])

        return destination.serialized

    transform = EodFundamentalTransformOperator(
        task_id="transform",
        adls_conn_id="azure_data_lake",
        source_path=XComGetter(task_id="one_fundamental.ingest"),
        destination_path=TempDirectory(
            base_dir=get_xcom_template(task_id="extract_security", key="temp_dir", use_map_index=False)
        ),
        entity_id=get_xcom_template(task_id="one_fundamental.ingest", key="entity_id"),
    )

    ingest(one_fundamental) >> transform


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    trigger_rule=TriggerRule.ALL_DONE,
    adls_conn_id="azure_data_lake",
    dataset_path={"container": "temp", "path": get_xcom_template(task_id="extract_security", key="temp_dir")},
    destination_path=FundamentalPath.curated(),
    pyarrow_options={
        "schema": pa.schema(
            [
                pa.field("entity_id", pa.string()),
                pa.field("currency", pa.string()),
                pa.field("metric", pa.string()),
                pa.field("value", pa.string()),
                pa.field("period_name", pa.string()),
                pa.field("period", pa.date32()),
                pa.field("published_at", pa.date32()),
                pa.field("year", pa.int32()),
            ]
        )
    },
    delta_table_options={
        "mode": "overwrite",
        # "partition_by": ["security_code", "exchange_code"],
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
    one_fundamental.expand(one_fundamental=extract_security()) >> sink


dag_object = fundamental()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
