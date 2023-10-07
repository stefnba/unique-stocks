# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
from airflow.decorators import task, dag
import pyarrow as pa

from typing import TypedDict
from custom.operators.data.transformation import DuckDbTransformationOperator, DataBindingCustomHandler

from shared.path import SecurityPath, ExchangePath, AdlsPath
from utils.dag.xcom import XComGetter
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from custom.providers.azure.hooks.handlers.read.azure import AzureDatasetArrowHandler

from custom.providers.azure.hooks import converters


class Exchange(TypedDict):
    code: str


@task
def extract_exchange():
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook
    import polars as pl

    dt = DeltaTableHook(conn_id="azure_data_lake").read(path=ExchangePath.curated())

    exchanges = pl.from_arrow(dt.to_pyarrow_dataset().to_batches())

    # return [
    #     {"exchange_code": "US"},
    #     {"exchange_code": "XETRA"},
    #     {"exchange_code": "GSE"},
    #     {"exchange_code": "VFEX"},
    #     # {"code": "F"},
    #     # {"code": "FOREX"},
    # ]

    if isinstance(exchanges, pl.DataFrame):
        return [
            *exchanges.select(
                [
                    pl.col("code").alias("exchange_code"),
                ]
            ).to_dicts(),
            {"exchange_code": "INDX"},  # add virtual exchange INDEX to get all index
        ]


@task
def ingest(security: list):
    from custom.hooks.api.shared import BulkApiHook

    api = BulkApiHook(
        conn_id="eod_historical_data",
        response_format="csv",
        adls_upload=BulkApiHook.AdlsUploadConfig(
            path=SecurityPath,
            conn_id="azure_data_lake",
            record_mapping={
                "exchange": "exchange_code",
            },
        ),
    )
    api.run(endpoint="exchange-symbol-list/${exchange_code}", items=security)

    # upload as arrow ds
    destination_path = api.upload_dataset(
        conn_id="azure_data_lake",
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

    return destination_path.to_dict()


transform = DuckDbTransformationOperator(
    task_id="transform",
    adls_conn_id="azure_data_lake",
    destination_path=AdlsPath.create_temp_file_path(),
    query="sql/security/transform.sql",
    data={
        "security_raw": DataBindingCustomHandler(
            path=XComGetter.pull_with_template(task_id="ingest"),
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
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["security"],
)
def security():
    extract_exchange_task = extract_exchange()
    ingest(extract_exchange_task) >> transform >> sink


dag_object = security()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
