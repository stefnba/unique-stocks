# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false


from datetime import datetime

from airflow.decorators import dag
from conf.spark import config as spark_config
from conf.spark import packages as spark_packages
from custom.providers.spark.operators.submit import SparkSubmitSHHOperator
from shared.connections import AWS_DATA_LAKE


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["seed", "ddl"],
)
def create_iceberg_tables():
    create_tables = SparkSubmitSHHOperator(
        task_id="create_iceberg_tables",
        ssh_conn_id="ssh_test",
        app_file_name="ddl.py",
        spark_conf={
            **spark_config.iceberg_hive_catalog,
        },
        spark_packages=[*spark_packages.iceberg],
        connections=[AWS_DATA_LAKE],
        conn_env_mapping={
            "AWS_ACCESS_KEY_ID": "AWS__LOGIN",
            "AWS_SECRET_ACCESS_KEY": "AWS__PASSWORD",
            "AWS_REGION": "AWS__EXTRA__REGION_NAME",
        },
    )

    seed_tables = SparkSubmitSHHOperator(
        task_id="seed_tables",
        ssh_conn_id="ssh_test",
        app_file_name="seed_tables.py",
        spark_conf={
            **spark_config.aws,
            **spark_config.iceberg_hive_catalog,
        },
        spark_packages=[*spark_packages.aws, *spark_packages.iceberg],
        connections=[AWS_DATA_LAKE],
        conn_env_mapping={
            "AWS_ACCESS_KEY_ID": "AWS__LOGIN",
            "AWS_SECRET_ACCESS_KEY": "AWS__PASSWORD",
            "AWS_REGION": "AWS__EXTRA__REGION_NAME",
        },
        dataset="s3a://uniquestocks/data-lake/seed/",
    )

    create_tables >> seed_tables


dag_object = create_iceberg_tables()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
