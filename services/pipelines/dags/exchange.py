# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag
import pyarrow as pa


from typing import TypedDict
from custom.operators.data.transformation import DuckDbTransformationOperator
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from shared.path import ExchangePath, AdlsPath
from utils.dag.xcom import XComGetter


class QuoteSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def ingest():
    from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook
    from shared.path import ExchangePath
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook

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
)


@dag(
    schedule=None,
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
