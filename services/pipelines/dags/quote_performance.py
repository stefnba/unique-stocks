# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false

import logging
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from conf.spark import config as spark_config
from conf.spark import packages as spark_packages
from custom.providers.spark.operators.submit import SparkSubmitSHHOperator
from dateutil.relativedelta import relativedelta  # type: ignore
from shared import airflow_dataset
from shared import connections as CONN
from utils.dag.xcom import XComGetter


@task
def sink_path():
    from shared.path import S3TempPath

    return S3TempPath.create_dir().uri


@task
def current_quote():
    from conf.spark import config as spark_config
    from conf.spark import packages as spark_packages
    from custom.providers.spark.hooks.submit import SparkSSHSubmitHook
    from shared.path import S3TempPath

    """Get the latest quote for all securities and save it as temporary file on S3."""

    CURRENT_DATE = date.today() - timedelta(days=1)
    temp_path = S3TempPath.create_dir().uri

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
        app_file_name="extract_latest_quote_by_date.py",
        env_vars=env,
        dataset=temp_path,
        conf={
            **spark_config.aws,
            **spark_config.iceberg_hive_catalog,
            "spark.referenceDate": CURRENT_DATE.strftime("%Y-%m-%d"),
        },
        packages=[*spark_packages.aws, *spark_packages.iceberg],
        py_files=["path.py"],
    )

    return temp_path


@task
def period():

    CURRENT_DATE = date.today() - timedelta(days=1)

    return [
        {
            "period": "YTD",
            "reference_date": date(CURRENT_DATE.year, 1, 1),
        },
        {
            "period": "L12M",
            "reference_date": date(CURRENT_DATE.year - 1, CURRENT_DATE.month, CURRENT_DATE.day),
        },
        {
            "period": "L3M",
            "reference_date": CURRENT_DATE - relativedelta(months=3),
        },
        # {
        #     "period": "L1M",
        #     "reference_date": CURRENT_DATE - relativedelta(months=1),
        # },
        # {
        #     "period": "L2M",
        #     "reference_date": CURRENT_DATE - relativedelta(months=2),
        # },
        # {
        #     "period": "L6M",
        #     "reference_date": CURRENT_DATE - relativedelta(months=6),
        # },
        # {
        #     "period": "L7D",
        #     "reference_date": CURRENT_DATE - timedelta(days=7),
        # },
    ]


@task(max_active_tis_per_dag=1)
def calculate(period):
    from conf.spark import config as spark_config
    from conf.spark import packages as spark_packages
    from custom.providers.spark.hooks.submit import SparkSSHSubmitHook
    from shared import connections as CONN
    from utils.dag.xcom import XComGetter

    reference_date = period.get("reference_date")
    period = period.get("period")

    logging.info(f"Calculating performance for period '{period}' and reference date '{reference_date}'.")

    """Get the latest quote for all securities and save it as temporary file on S3."""

    CURRENT_DATE = date.today() - timedelta(days=1)

    sink_path = XComGetter.pull_now("sink_path", use_map_index=False)

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
        app_file_name="extract_latest_quote_by_date.py",
        env_vars=env,
        dataset=sink_path,
        conf={
            **spark_config.aws,
            **spark_config.iceberg_hive_catalog,
            "spark.referenceDate": reference_date.strftime("%Y-%m-%d"),
            "spark.period": period,
        },
        packages=[*spark_packages.aws, *spark_packages.iceberg],
        py_files=["path.py"],
    )


sink = SparkSubmitSHHOperator(
    task_id="sink_to_iceberg",
    app_file_name="quote_performance/sink.py",
    ssh_conn_id="ssh_test",
    spark_conf={
        **spark_config.adls,
        **spark_config.iceberg_hive_catalog,
        "spark.currentPath": XComGetter.pull_with_template("current_quote"),
        "spark.referencePath": XComGetter.pull_with_template("sink_path"),
    },
    spark_packages=[*spark_packages.adls, *spark_packages.iceberg],
    connections=[CONN.AWS_DATA_LAKE, CONN.AZURE_DATA_LAKE],
    dataset=XComGetter.pull_with_template("sink_path"),
    conn_env_mapping={
        "AWS_ACCESS_KEY_ID": "AWS__LOGIN",
        "AWS_SECRET_ACCESS_KEY": "AWS__PASSWORD",
        "AWS_REGION": "AWS__EXTRA__REGION_NAME",
        "ADLS_STORAGE_ACCOUNT_NAME": "AZURE_DATA_LAKE__HOST",
        "ADLS_CLIENT_ID": "AZURE_DATA_LAKE__LOGIN",
        "ADLS_CLIENT_SECRET": "AZURE_DATA_LAKE__PASSWORD",
        "ADLS_TENANT_ID": "AZURE_DATA_LAKE__EXTRA__TENANT_ID",
    },
    outlets=[airflow_dataset.Security],
)


@dag(
    schedule=[airflow_dataset.SecurityQuote],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["quote"],
)
def quote_performance():
    sink_path_task = sink_path()
    period_task = period()
    current_quote_task = current_quote()

    calculate_task = calculate.expand(period=period_task)

    sink_path_task >> current_quote_task >> period_task >> calculate_task >> sink


dag_object = quote_performance()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
