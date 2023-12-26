# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime
from typing import TypedDict

import pyarrow as pa
from airflow.decorators import dag, task
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from custom.operators.data.transformation import DuckDbTransformationOperator
from shared import airflow_dataset
from shared.path import AdlsPath, ExchangePath
from utils.dag.xcom import XComGetter


class QuoteSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def ingest():
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
    from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook
    from shared.path import ExchangePath

    exchange_raw = EodHistoricalDataApiHook().exchange()

    destination = ExchangePath.raw(source="EodHistoricalData", format="json")

    hook = AzureDataLakeStorageHook(conn_id="azure_data_lake")
    hook.upload(**destination.afls_path, data=exchange_raw)

    return destination.to_dict()


transform = DuckDbTransformationOperator(
    task_id="transform",
    adls_conn_id="azure_data_lake",
    destination_path=AdlsPath.create_temp_file_path(),
    query="sql/exchange/transform.sql",
    data={"exchange": XComGetter.pull_with_template(task_id="ingest")},
)


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    adls_conn_id="azure_data_lake",
    dataset_path=XComGetter.pull_with_template(task_id="transform"),
    destination_path=ExchangePath.curated(),
    pyarrow_options={
        "schema": pa.schema(
            [
                pa.field("name", pa.string()),
                pa.field("code", pa.string()),
                pa.field("operating_mic", pa.string()),
                pa.field("currency", pa.string()),
                pa.field("country", pa.string()),
            ]
        )
    },
    delta_table_options={
        "mode": "overwrite",
    },
    outlets=[airflow_dataset.Exchange],
)


@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["exchange"],
)
def exchange():
    ingest() >> transform >> sink


dag_object = exchange()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
