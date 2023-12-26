# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false

from datetime import datetime

from airflow.decorators import dag
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from custom.providers.eod_historical_data.operators.transformers import EoDQuotePerformanceTransformOperator
from shared import airflow_dataset, schema
from shared.path import SecurityQuotePath, SecurityQuotePerformancePath
from utils.dag.xcom import XComGetter


@dag(
    schedule=[airflow_dataset.SecurityQuote],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["quote"],
)
def quote_performance():
    transform = EoDQuotePerformanceTransformOperator(
        task_id="transform",
        adls_conn_id="azure_data_lake",
        dataset_path=SecurityQuotePath.curated(),
    )

    sink = WriteDeltaTableFromDatasetOperator(
        task_id="sink",
        adls_conn_id="azure_data_lake",
        dataset_path=XComGetter.pull_with_template(task_id="transform"),
        destination_path=SecurityQuotePerformancePath.curated(),
        pyarrow_options={
            "schema": schema.QuotePerformance,
        },
        delta_table_options={
            "mode": "overwrite",
            "schema": schema.QuotePerformance,
        },
    )
    transform >> sink


dag_object = quote_performance()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
