# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
import logging
from datetime import datetime, timedelta
from typing import TypedDict

from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from conf.spark import config as spark_config
from conf.spark import packages as spark_packages
from custom.providers.spark.operators.submit import SparkSubmitSHHOperator
from shared import connections as CONN
from shared.path import ADLSRawZonePath
from utils.dag.xcom import XComGetter
from utils.filesystem.directory import DirFile

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


class FundamentalSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def extract_security():
    """
    Get all securities and map their exchange composite_code.
    """
    import polars as pl
    from custom.providers.iceberg.hooks.pyiceberg import IcebergStaticTableHook, filter_expressions
    from utils.dag.conf import get_dag_conf

    conf = get_dag_conf()
    exchanges = conf.get("exchanges", ["XETRA", "NASDAQ", "NYSE"])
    security_types = conf.get("security_types", ["common_stock"])

    # Filter out indexes since we get their fundamentals, i.e. members, with different DAG
    exchanges = [e for e in exchanges if e != "INDX"]
    security_types = [e for e in security_types if e != "index"]

    logging.info(
        f"""Getting fundamentals for exchanges: '{", ".join(exchanges)}' and"""
        f"""security types: '{", ".join(security_types)}'."""
    )

    mapping = IcebergStaticTableHook(
        catalog_conn_id=CONN.ICEBERG_CATALOG,
        io_conn_id=CONN.AWS_DATA_LAKE,
        catalog_name="uniquestocks",
        table_name=("mapping", "mapping"),
    ).to_polars(
        selected_fields=("source_value", "mapping_value"),
        row_filter="field = 'composite_code' AND product = 'exchange' AND source = 'EodHistoricalData'",
    )
    security = IcebergStaticTableHook(
        catalog_conn_id=CONN.ICEBERG_CATALOG,
        io_conn_id=CONN.AWS_DATA_LAKE,
        catalog_name="uniquestocks",
        table_name=("curated", "security"),
    ).to_polars(
        selected_fields=("exchange_code", "code", "type"),
        row_filter=filter_expressions.In("exchange_code", exchanges),
    )

    security = security.join(mapping, left_on="exchange_code", right_on="source_value", how="left").with_columns(
        pl.coalesce(["mapping_value", "exchange_code"]).alias("api_exchange_code"),
    )

    securities_as_dict = []

    # create a list of securities for each exchange and security type
    for e in exchanges:
        for t in security_types:
            s = security.filter((pl.col("exchange_code") == e) & (pl.col("type") == t)).to_dicts()

            if len(s) > 0:
                securities_as_dict.append(s)

    return securities_as_dict


@task
def set_ingest_sink_path():
    """
    Set sink directory on ADLS for ingestion.
    """

    sink_path = ADLSRawZonePath(product="fundamental", source="EodHistoricalData", type="json")

    return f"{sink_path.path.scheme}://{sink_path.path.container}/{sink_path.path.dirs}"


@task
def set_post_transform_sink_path():
    """
    Set temporary sink directory on S3. Transformed data will be written to this directory and
    Spark sink job will read from this directory.
    """
    from shared.path import S3TempPath

    sink_path = S3TempPath.create_dir()

    return sink_path.uri


@task_group
def ingest_security_group(security_groups):
    """
    Ingest quotes for a list of securities from a specific exchange and with a specific type.
    """

    @task
    def map_url_sink_path(securities):
        """
        Take list of securities and add endpoint and blob path to each record.
        """

        from airflow.exceptions import AirflowException
        from airflow.models.taskinstance import TaskInstance
        from airflow.operators.python import get_current_context
        from shared.path import ADLSRawZonePath

        context = get_current_context()

        ti = context.get("task_instance")

        if not isinstance(ti, TaskInstance):
            raise AirflowException("No task instance found.")

        path = ADLSRawZonePath.parse(path=ti.xcom_pull(task_ids="set_ingest_sink_path"), extension="json")

        return list(
            map(
                lambda exchange: {
                    "endpoint": f"{exchange['code']}.{exchange['api_exchange_code']}",
                    "blob": path.add_partition(
                        {"exchange": exchange["exchange_code"], "security": exchange["code"]}
                    ).blob,
                },
                securities,
            )
        )

    @task(max_active_tis_per_dag=1)
    def ingest(securities):
        from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook, UrlUploadRecord
        from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook

        logging.info(f"""Ingest of securities for exchange '{securities[0].get("exchange_code", "")}'.""")

        hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
        api_hook = EodHistoricalDataApiHook()

        url_endpoints = [UrlUploadRecord(**sec) for sec in securities[:10]]

        uploaded_blobs = hook.upload_from_url(
            container="raw",
            base_url="https://eodhistoricaldata.com/api/fundamentals",
            base_params=api_hook._base_params,
            url_endpoints=url_endpoints,
        )

        return uploaded_blobs

    @task
    def download_transform(blobs):
        from uuid import uuid4

        from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook
        from custom.providers.duckdb.hooks.query import DuckDBQueryHook
        from shared.path import LocalStoragePath
        from utils.filesystem.directory import scan_dir_files
        from utils.parallel.concurrent import proces_paralell

        hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")

        download_dir = LocalStoragePath.create_dir()
        sink_dir = LocalStoragePath.create_dir()

        hook.download_blobs_from_list(
            container="raw",
            blobs=blobs,
            destination_dir=download_dir.full_path,
        )

        logging.info(f"""Downloaded {len(blobs)} blobs to '{download_dir.full_path}'.""")

        files = scan_dir_files(download_dir.full_path)

        proces_paralell(task=transform, iterable=files, sink_dir=sink_dir.full_path)

        logging.info(f"""Transformed files to '{sink_dir.full_path}'.""")

        duck = DuckDBQueryHook(conn_id="aws")
        data = duck.duck.read_parquet(f"{sink_dir.full_path}/*.parquet")

        upload_path = XComGetter.pull_now(task_id="set_post_transform_sink_path", use_map_index=False)

        logging.info(f"""Writing transformed data to S3 path '{upload_path}'.""")

        data.write_parquet(
            upload_path + "/" + uuid4().hex + ".parquet",
            compression=None,
        )

    map_task = map_url_sink_path(security_groups)
    ingest_task = ingest(map_task)
    download_transform(ingest_task)


def transform(file: DirFile, sink_dir: str):
    import json
    import uuid
    from pathlib import Path

    from custom.providers.eod_historical_data.transformers.fundamental.common_stock import (
        EoDCommonStockFundamentalTransformer,
    )
    from custom.providers.eod_historical_data.transformers.utils import deep_get

    data = Path(file.path).read_text()

    data_dict = json.loads(data)
    sec_type = deep_get(data_dict, ["General", "Type"])

    transformed_data = None

    if sec_type == "Common Stock":
        transformer = EoDCommonStockFundamentalTransformer(data=data)
        transformed_data = transformer.transform()

    if transformed_data is not None:
        sink_path = Path(sink_dir) / f"{uuid.uuid4().hex}.parquet"
        transformed_data.write_parquet(sink_path)


sink = SparkSubmitSHHOperator(
    task_id="sink_to_iceberg",
    app_file_name="sink_fundamental.py",
    ssh_conn_id="ssh_test",
    spark_conf={
        **spark_config.aws,
        **spark_config.iceberg_jdbc_catalog,
    },
    spark_packages=[*spark_packages.aws, *spark_packages.iceberg],
    connections=[CONN.AWS_DATA_LAKE],
    dataset=XComGetter.pull_with_template(task_id="set_post_transform_sink_path"),
    conn_env_mapping={
        "AWS_ACCESS_KEY_ID": "AWS__LOGIN",
        "AWS_SECRET_ACCESS_KEY": "AWS__PASSWORD",
        "AWS_REGION": "AWS__EXTRA__REGION_NAME",
    },
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["fundamental"],
    default_args=default_args,
)
def fundamental():
    extract_task = extract_security()
    set_ingest_sink_task = set_ingest_sink_path()
    set_sink_post_transform_task = set_post_transform_sink_path()

    ingest_task = ingest_security_group.expand(security_groups=extract_task)

    extract_task >> [set_ingest_sink_task, set_sink_post_transform_task] >> ingest_task >> sink


dag_object = fundamental()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(
        conn_file_path=connections,
        run_conf={
            "delta_table_mode": "overwrite",
            "exchanges": ["XETRA", "NASDAQ", "NYSE"],
            "security_types": [
                "common_stock",
            ],
        },
    )
