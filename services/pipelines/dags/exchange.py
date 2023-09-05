# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag, task_group


from typing import TypedDict
from custom.operators.data.transformation import DuckDbTransformationOperator

from shared.data_lake_path import SecurityQuotePath, TempDirectory
from utils.dag.xcom import XComGetter, set_xcom_value, get_xcom_template, get_xcom_value


class QuoteSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def ingest():
    from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook
    from shared.data_lake_path import ExchangePath
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook

    exchange_raw = EodHistoricalDataApiHook().exchange()

    destination = ExchangePath.raw(source="EodHistoricalData", format="json")

    hook = AzureDataLakeStorageHook(conn_id="azure_data_lake")
    hook.upload(container=destination.container, blob_path=destination.path, data=exchange_raw)

    return destination.serialized


transform = DuckDbTransformationOperator(
    task_id="transform",
    adls_conn_id="azure_data_lake",
    destination_path="test.parquet",
    destination_container="temp",
    query="sql/exchange/transform.sql",
    data={"exchange": get_xcom_template(task_id="ingest")},
)


@task
def sink():
    from pyarrow import dataset as ds
    import pyarrow as pa

    from deltalake.writer import write_deltalake
    from adlfs import AzureBlobFileSystem

    filesystem = AzureBlobFileSystem(account_name="uniquestocksdatalake", anon=False)

    storage_options = {}

    base_dir = get_xcom_value(task_id="extract_security", key="temp_dir")

    schema = pa.schema(
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

    ds = ds.dataset(f"temp/{base_dir}", filesystem=filesystem, format="parquet", schema=schema)

    write_deltalake(
        "abfs://curated/security_quote",
        data=ds.to_batches(),
        schema=ds.schema,
        storage_options=storage_options,
        mode="overwrite",
        # partition_by=["security_code", "exchange_code"],
        # overwrite_schema=True,
    )

    return base_dir


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["exchange"],
)
def exchange():
    ingest() >> transform


dag_object = exchange()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
