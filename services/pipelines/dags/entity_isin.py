# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import dag, task
from conf.spark import config as spark_config
from conf.spark import packages as spark_packages
from custom.providers.spark.operators.submit import SparkSubmitSHHOperator
from shared import airflow_dataset
from shared import connections as CONN
from utils.dag.xcom import XComGetter

URL = "https://mapping.gleif.org/api/v2/isin-lei/d6996d23-cdaf-413e-b594-5219d40f3da5/download"


@task
def ingest():
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from custom.hooks.http.hook import HttpHook
    from shared.path import S3RawZonePath
    from utils.file.unzip import compress_with_gzip, unzip_file

    # download to file
    local_file = HttpHook().stream_to_local_file(url=URL, file_type="zip")

    # rezip
    unzipped_file_path = unzip_file(local_file)
    rezipped_path = compress_with_gzip(unzipped_file_path.uri)

    # ingest to data lake
    ingest_path = S3RawZonePath(source="Gleif", type="csv.gz", product="entity_isin")
    hook = S3Hook(aws_conn_id=CONN.AWS_DATA_LAKE)
    hook.load_file(filename=rezipped_path, key=ingest_path.key, bucket_name=ingest_path.bucket)

    # todo remove file rezipped_path

    return ingest_path.uri


transform_sink = SparkSubmitSHHOperator(
    task_id="transform_sink_to_iceberg",
    app_file_name="sink_entity_isin.py",
    ssh_conn_id="ssh_test",
    spark_conf={
        **spark_config.aws,
        **spark_config.iceberg_hive_catalog,
    },
    spark_packages=[*spark_packages.aws, *spark_packages.iceberg],
    connections=[CONN.AWS_DATA_LAKE, CONN.ICEBERG_CATALOG],
    dataset=XComGetter.pull_with_template("ingest"),
    conn_env_mapping={
        "AWS_ACCESS_KEY_ID": "AWS__LOGIN",
        "AWS_SECRET_ACCESS_KEY": "AWS__PASSWORD",
        "AWS_REGION": "AWS__EXTRA__REGION_NAME",
    },
)


@dag(
    schedule=[airflow_dataset.Entity],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["entity"],
)
def entity_isin():
    ingest_task = ingest()
    ingest_task >> transform_sink


dag_object = entity_isin()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
