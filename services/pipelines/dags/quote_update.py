# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from conf.spark import config as spark_config
from conf.spark import packages as spark_packages
from custom.providers.spark.operators.submit import SparkSubmitSHHOperator
from shared import connections as CONN
from shared.path import S3TempPath
from utils.dag.xcom import XComGetter

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


@task
def extract_latest_date_by_exchange():
    """
    For each exchange, extract the latest date for which we have quotes using Spark SQL.
    """

    from custom.providers.spark.hooks.submit import SparkSSHSubmitHook

    path = S3TempPath.create_dir()

    hook = SparkSSHSubmitHook(ssh_conn_id="ssh_test")
    env = hook.get_env_from_conn(
        connections=[CONN.AWS_DATA_LAKE],
        mapping={
            "AWS_ACCESS_KEY_ID": "AWS__LOGIN",
            "AWS_SECRET_ACCESS_KEY": "AWS__PASSWORD",
            "AWS_REGION": "AWS__EXTRA__REGION_NAME",
        },
    )
    hook.submit(
        app_file_name="extract_latest_date_by_exchange.py",
        env_vars=env,
        dataset=path.uri,
        conf={
            **spark_config.aws,
            **spark_config.iceberg_jdbc_catalog,
        },
        packages=[*spark_packages.aws, *spark_packages.iceberg],
    )

    return path.uri


@task
def missing_dates(path):
    from custom.providers.duckdb.hooks.query import DuckDBQueryHook

    duck = DuckDBQueryHook(conn_id=CONN.AWS_DATA_LAKE)

    data = (
        duck.query(
            """
        SELECT
            exchange_code,
            list_transform(generate_series(
                (latest_date + 1)::timestamp, (current_date()-1)::timestamp, interval '1 day'
            ), x -> x::date::varchar) AS dates
        FROM read_parquet('$path')""",
            {"path": f"{path}/**.parquet"},
        )
        .pl()
        .to_dicts()
    )

    return data


@task
def set_sink_path():
    from shared.path import ADLSRawZonePath

    sink_path = ADLSRawZonePath(product="security_quote", source="EodHistoricalData", type="csv")

    return f"{sink_path.path.scheme}://{sink_path.path.container}/{sink_path.path.dirs}"


@task_group
def ingest_quote_group(exchange_groups: dict):

    @task
    def map_url_sink_path(record: dict):

        from airflow.exceptions import AirflowException
        from airflow.models.taskinstance import TaskInstance
        from airflow.operators.python import get_current_context
        from shared.path import ADLSRawZonePath

        context = get_current_context()
        ti = context.get("task_instance")

        if not isinstance(ti, TaskInstance):
            raise AirflowException("No task instance found.")

        exchange_code = record.get("exchange_code")
        if not exchange_code:
            raise AirflowException("No exchange code found.")

        dates = record.get("dates", [])

        path = ADLSRawZonePath.parse(path=ti.xcom_pull(task_ids="set_sink_path"), extension="csv")

        return list(
            map(
                lambda date: {
                    "endpoint": f"{exchange_code}?date={date}",
                    "blob": path.add_partition({"exchange": exchange_code}).blob,
                },
                dates,
            )
        )

    @task
    def ingest(dates):
        from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook, UrlUploadRecord
        from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook

        hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
        api_hook = EodHistoricalDataApiHook()

        url_endpoints = [UrlUploadRecord(**sec) for sec in dates]

        uploaded_blobs = hook.upload_from_url(
            container="raw",
            base_url="https://eodhistoricaldata.com/api/eod-bulk-last-day",
            base_params=api_hook._base_params,
            url_endpoints=url_endpoints,
        )

        return uploaded_blobs

    map_task = map_url_sink_path(exchange_groups)
    ingest(map_task)


@task
def set_transform_sink_path():
    from shared.path import ADLSTempPath

    sink_path = ADLSTempPath.create_dir()
    return sink_path.uri


transform = SparkSubmitSHHOperator(
    task_id="transform",
    app_file_name="transform_security_quote.py",
    ssh_conn_id="ssh_test",
    spark_conf={**spark_config.adls, "spark.sink": XComGetter.pull_with_template(task_id="set_transform_sink_path")},
    spark_packages=[*spark_packages.adls],
    connections=[CONN.AZURE_DATA_LAKE],
    dataset=XComGetter.pull_with_template(task_id="set_sink_path"),
    conn_env_mapping={
        "ADLS_STORAGE_ACCOUNT_NAME": "AZURE_DATA_LAKE__HOST",
        "ADLS_CLIENT_ID": "AZURE_DATA_LAKE__LOGIN",
        "ADLS_CLIENT_SECRET": "AZURE_DATA_LAKE__PASSWORD",
        "ADLS_TENANT_ID": "AZURE_DATA_LAKE__EXTRA__TENANT_ID",
    },
    py_files=["path.py"],
)


sink = SparkSubmitSHHOperator(
    task_id="sink_to_iceberg",
    app_file_name="sink_security_quote.py",
    ssh_conn_id="ssh_test",
    spark_conf={
        **spark_config.adls,
        **spark_config.iceberg_hive_catalog,
    },
    spark_packages=[*spark_packages.adls, *spark_packages.iceberg],
    connections=[CONN.AWS_DATA_LAKE, CONN.AZURE_DATA_LAKE],
    dataset=XComGetter.pull_with_template(task_id="set_transform_sink_path"),
    conn_env_mapping={
        "AWS_ACCESS_KEY_ID": "AWS__LOGIN",
        "AWS_SECRET_ACCESS_KEY": "AWS__PASSWORD",
        "AWS_REGION": "AWS__EXTRA__REGION_NAME",
        "ADLS_STORAGE_ACCOUNT_NAME": "AZURE_DATA_LAKE__HOST",
        "ADLS_CLIENT_ID": "AZURE_DATA_LAKE__LOGIN",
        "ADLS_CLIENT_SECRET": "AZURE_DATA_LAKE__PASSWORD",
        "ADLS_TENANT_ID": "AZURE_DATA_LAKE__EXTRA__TENANT_ID",
    },
    py_files=["path.py"],
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["quote", "security"],
    default_args=default_args,
)
def quote_update():
    extract_task = extract_latest_date_by_exchange()
    dates_task = missing_dates(extract_task)
    set_sink_task = set_sink_path()

    ingest_task = ingest_quote_group.expand(exchange_groups=dates_task)
    set_transform_sink_path_task = set_transform_sink_path()

    dates_task >> set_sink_task >> ingest_task >> set_transform_sink_path_task >> transform >> sink


dag_object = quote_update()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(
        conn_file_path=connections,
        run_conf={
            "delta_table_mode": "overwrite",
            "exchanges": ["XETRA", "NASDAQ"],
            "security_types": [
                "common_stock",
            ],
        },
    )
