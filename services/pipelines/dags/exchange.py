# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import dag, task
from conf.spark import config as spark_config
from conf.spark import packages as spark_packages
from custom.providers.duckdb.operators.transform import DuckDbTransformOperator
from custom.providers.spark.operators.submit import SparkSubmitSHHOperator
from shared import airflow_dataset
from shared.path import S3RawZonePath, S3TempPath
from utils.dag.xcom import XComGetter

AWS_DATA_LAKE_CONN_ID = "aws"
AZURE_DATA_LAKE_CONN_ID = "azure_data_lake"


@task
def ingest():
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook

    exchange_raw = EodHistoricalDataApiHook().exchange()

    destination = S3RawZonePath(source="EodHistoricalData", type="json", product="exchange")

    hook = S3Hook(aws_conn_id=AWS_DATA_LAKE_CONN_ID)
    hook.load_string(string_data=exchange_raw, key=destination.key, bucket_name=destination.bucket)

    return destination.uri


transform = DuckDbTransformOperator(
    task_id="transform",
    conn_id=AWS_DATA_LAKE_CONN_ID,
    destination_path=S3TempPath.create_file().uri,
    query="sql/exchange/transform.sql",
    query_params={"exchange": XComGetter.pull_with_template("ingest")},
)


sink = SparkSubmitSHHOperator(
    task_id="sink_to_iceberg",
    app_file_name="sink_exchange.py",
    ssh_conn_id="ssh_test",
    spark_conf={
        **spark_config.aws,
        **spark_config.iceberg,
    },
    spark_packages=[*spark_packages.aws, *spark_packages.iceberg],
    connections=[AWS_DATA_LAKE_CONN_ID, AZURE_DATA_LAKE_CONN_ID],
    dataset=XComGetter.pull_with_template("transform"),
    conn_env_mapping={
        "AWS_ACCESS_KEY_ID": "AWS__LOGIN",
        "AWS_SECRET_ACCESS_KEY": "AWS__PASSWORD",
        "AWS_REGION": "AWS__EXTRA__REGION_NAME",
        "ADLS_STORAGE_ACCOUNT_NAME": "AZURE_DATA_LAKE__HOST",
        "ADLS_CLIENT_ID": "AZURE_DATA_LAKE__LOGIN",
        "ADLS_CLIENT_SECRET": "AZURE_DATA_LAKE__PASSWORD",
        "ADLS_TENANT_ID": "AZURE_DATA_LAKE__EXTRA__TENANT_ID",
    },
    outlets=[airflow_dataset.Exchange],
)


@dag(
    schedule="@weekly",
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
