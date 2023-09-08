# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag, task_group
from airflow.utils.trigger_rule import TriggerRule
import pyarrow as pa

from typing import TypedDict
from custom.operators.data.transformation import DuckDbTransformationOperator

from shared.data_lake_path import SecurityQuotePath, TempDirectory
from utils.dag.xcom import XComGetter, set_xcom_value, get_xcom_template
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator


class QuoteSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def extract_security():
    import polars as pl
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook

    set_xcom_value(key="temp_dir", value=TempDirectory().directory)

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

        return s

    # return [
    #     {"exchange_code": "US", "security_code": "AAPL"},
    #     {"exchange_code": "F", "security_code": "APC"},
    #     {"exchange_code": "XETRA", "security_code": "APC"},
    #     {"exchange_code": "US", "security_code": "MFST"},
    # ]


@task_group
def one_security(one_security: QuoteSecurity):
    @task()
    def ingest(security: QuoteSecurity):
        from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
        import json

        data = EodHistoricalDataApiHook().historical_quote(**security)

        destination = SecurityQuotePath.raw(source="EodHistoricalData", format="json").add_element(
            security=security["security_code"], exchange=security["exchange_code"]
        )
        WasbHook(wasb_conn_id="azure_blob").upload(
            data=json.dumps(data),
            container_name=destination.container,
            blob_name=destination.path,
        )

        set_xcom_value("exchange_code", security["exchange_code"])
        set_xcom_value("security_code", security["security_code"])

        return destination.serialized

    transform = DuckDbTransformationOperator(
        task_id="transform",
        adls_conn_id="azure_data_lake",
        destination_path=TempDirectory(
            base_dir=get_xcom_template(task_id="extract_security", key="temp_dir", use_map_index=False)
        ),
        query="sql/historical_quote/transform.sql",
        data={"quotes_raw": get_xcom_template(task_id="one_security.ingest")},
        query_args={
            "security_code": XComGetter(task_id="one_security.ingest", key="security_code"),
            "exchange_code": XComGetter(task_id="one_security.ingest", key="exchange_code"),
        },
    )

    ingest_task = ingest(one_security)
    ingest_task >> transform


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    trigger_rule=TriggerRule.ALL_DONE,
    adls_conn_id="azure_data_lake",
    dataset_path={"container": "temp", "path": get_xcom_template(task_id="extract_security", key="temp_dir")},
    destination_path=SecurityQuotePath.curated(),
    pyarrow_options={
        "schema": pa.schema(
            [
                pa.field("date", pa.string()),
                pa.field("open", pa.float64()),
                pa.field("high", pa.float64()),
                pa.field("low", pa.float64()),
                pa.field("close", pa.float64()),
                pa.field("adjusted_close", pa.float64()),
                pa.field("volume", pa.int64()),
                pa.field("exchange_code", pa.string()),
                pa.field("security_code", pa.string()),
            ]
        )
    },
    delta_table_options={
        "mode": "overwrite",
        "partition_by": ["security_code", "exchange_code"],
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
    one_security.expand(one_security=extract_security()) >> sink


dag_object = historical_quote()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
