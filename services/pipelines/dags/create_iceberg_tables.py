# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false


from datetime import datetime

from airflow.decorators import dag
from custom.providers.spark.operators.submit import SparkSubmitSHHOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["security"],
)
def create_iceberg_tables():
    SparkSubmitSHHOperator(
        task_id="create_iceberg_tables",
        ssh_conn_id="ssh_test",
        app_file_name="ddl.py",
        spark_conf={
            "spark.sql.catalog.uniquestocks_dev": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.uniquestocks_dev.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
            "spark.sql.catalog.uniquestocks_dev.warehouse": "s3a://uniquestocks-datalake-dev/curated/iceberg/",
            "spark.sql.catalog.uniquestocks_dev.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        },
        spark_packages=[
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
            "org.apache.iceberg:iceberg-aws-bundle:1.4.3",
        ],
        connections=["aws"],
        conn_env_mapping={
            "AWS_ACCESS_KEY_ID": "AWS__LOGIN",
            "AWS_SECRET_ACCESS_KEY": "AWS__PASSWORD",
            "AWS_REGION": "AWS__EXTRA__REGION_NAME",
        },
    )


dag_object = create_iceberg_tables()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
