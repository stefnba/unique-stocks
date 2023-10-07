# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag


from typing import TypedDict


from shared.path import SecurityQuotePath, SecurityPath, AdlsPath
from utils.dag.xcom import XComGetter
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from custom.operators.data.transformation import DuckDbTransformationOperator, DataBindingCustomHandler
from custom.hooks.api.shared import BulkApiHook
from custom.providers.azure.hooks.handlers.read.azure import AzureDatasetArrowHandler

from custom.providers.azure.hooks import converters
from shared import schema


class QuoteSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def extract_security():
    import polars as pl
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook

    dt = DeltaTableHook(conn_id="azure_data_lake").read(path=SecurityPath.curated())

    securities = pl.from_arrow(
        dt.to_pyarrow_dataset(
            partitions=[
                (
                    "exchange_code",
                    "in",
                    [
                        # "XETRA",
                        # "LSE",
                        # "NASDAQ",
                        "NYSE",
                    ],
                ),
                ("type", "=", "common_stock"),
            ]
        ).to_batches()
    )

    if isinstance(securities, pl.DataFrame):
        s = (
            securities.select(
                [
                    pl.lit("US").alias("composite_exchange_code"),
                    pl.col("exchange_code"),
                    pl.col("code").alias("security_code"),
                ]
            )
            # .head(10)
            .to_dicts()
        )

        return s


@task
def ingest(securities: list[dict[str, str]]):
    api = BulkApiHook(
        conn_id="eod_historical_data",
        response_format="csv",
        adls_upload=BulkApiHook.AdlsUploadConfig(
            path=SecurityQuotePath,
            conn_id="azure_data_lake",
            record_mapping={
                "exchange": "exchange_code",
                "security": "security_code",
            },
        ),
    )
    api.run(endpoint="eod/${security_code}.${composite_exchange_code}", items=securities)

    # upload as arrow ds
    destination_path = api.upload_dataset(conn_id="azure_data_lake")

    return destination_path.to_dict()


transform = DuckDbTransformationOperator(
    task_id="transform",
    adls_conn_id="azure_data_lake",
    destination_path=AdlsPath.create_temp_file_path(),
    query="sql/historical_quote/transform.sql",
    data={
        "quotes_raw": DataBindingCustomHandler(
            path=XComGetter.pull_with_template(task_id="ingest"),
            handler=AzureDatasetArrowHandler,
            dataset_converter=converters.PyArrowDataset,
        )
    },
)

sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    adls_conn_id="azure_data_lake",
    dataset_path=XComGetter.pull_with_template(task_id="transform"),
    destination_path=SecurityQuotePath.curated(),
    pyarrow_options={
        "schema": schema.SecurityQuote,
    },
    delta_table_options={
        "mode": "overwrite",
        "schema": schema.SecurityQuote,
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
    extract_security_task = extract_security()

    ingest(extract_security_task) >> transform >> sink


dag_object = historical_quote()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
