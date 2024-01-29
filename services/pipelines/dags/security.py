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
from shared import connections as conn
from shared.path import ADLSRawZonePath, ADLSTempPath
from utils.dag.xcom import XComGetter


@task
def extract_exchange_codes():
    import polars as pl
    from custom.providers.iceberg.hooks.pyiceberg import IcebergStaticTableHook

    exchanges = IcebergStaticTableHook(
        io_conn_id=conn.AWS_DATA_LAKE,
        catalog_conn_id=conn.ICEBERG_CATALOG,
        catalog_name="uniquestocks",
        table_name=("curated", "exchange"),
    ).to_polars()

    return [
        *exchanges.select(
            [
                pl.col("code").alias("exchange_code"),
            ]
        ).to_dicts(),
        {"exchange_code": "INDX"},  # add virtual exchange INDEX to get all index
    ]


@task
def map_url_sink_path(exchanges):
    """
    Map url and destination path to each record.

    params:
        exchanges: list of exchange codes, each record is a dict with key `exchange_code`.

    returns:
        list of dict with keys `endpoint` and `blob` where `blob` is the destination path.
    """

    from utils.dag.xcom import XComSetter

    sink_path = ADLSRawZonePath(product="security", source="EodHistoricalData", type="csv")

    XComSetter.set(key="sink_path", value=f"{sink_path.path.scheme}://{sink_path.path.container}/{sink_path.path.dirs}")

    return list(
        map(
            lambda exchange: {
                "endpoint": exchange["exchange_code"],
                "blob": sink_path.add_partition({"exchange": exchange["exchange_code"]}).blob,
            },
            exchanges,
        )
    )


@task
def ingest(exchanges):
    import logging

    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook, UrlUploadRecord
    from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook

    logging.info(f"""Ingest of securities of {len(exchanges)} exchanges.""")

    hook = AzureDataLakeStorageBulkHook(conn_id=conn.AZURE_DATA_LAKE)
    api_hook = EodHistoricalDataApiHook()

    url_endpoints = [UrlUploadRecord(**exchange) for exchange in exchanges]

    uploaded_blobs = hook.upload_from_url(
        container="raw",
        base_url="https://eodhistoricaldata.com/api/exchange-symbol-list",
        base_params=api_hook._base_params,
        url_endpoints=url_endpoints,
    )

    return uploaded_blobs


transform = DuckDbTransformOperator(
    task_id="transform",
    conn_id=conn.AZURE_DATA_LAKE,
    destination_path=ADLSTempPath.create_file().uri,
    query="sql/security/transform.sql",
    query_params={"securities": XComGetter.pull_with_template(task_id="map_url_sink_path", key="sink_path")},
)


sink = SparkSubmitSHHOperator(
    task_id="sink_to_iceberg",
    app_file_name="sink_security.py",
    ssh_conn_id="ssh_test",
    spark_conf={
        **spark_config.adls,
        **spark_config.iceberg_jdbc_catalog,
    },
    spark_packages=[*spark_packages.adls, *spark_packages.iceberg],
    connections=[conn.AWS_DATA_LAKE, conn.AZURE_DATA_LAKE],
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
)


@dag(
    schedule=[airflow_dataset.Exchange],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["security"],
)
def security():
    extract_task = extract_exchange_codes()
    map_task = map_url_sink_path(extract_task)
    ingest_task = ingest(map_task)

    ingest_task >> transform >> sink


dag_object = security()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
