# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import dag
from custom.providers.spark.operators.submit import SparkSubmitSHHOperator
from shared import airflow_dataset


@dag(
    schedule=[airflow_dataset.Exchange],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["security"],
)
def test_spark():
    # @task
    # def test():
    #     from custom.providers.spark.hooks.submit import SparkSSHSubmitHook

    #     hook = SparkSSHSubmitHook("ssh_test")

    #     env = hook.get_env_from_conn(
    #         ["azure_data_lake"], mapping={"ADLS_STORAGE_ACCOUNT_NAME": "AZURE_DATA_LAKE__HOST"}
    #     )

    #     hook.submit(
    #         app_file_name="test.py",
    #         py_files=[
    #             "path.py",
    #         ],
    #         env_vars=env,
    #         dataset="abfs://raw/haaaaaaaaaallllllooooooo/nein.zip",
    #     )

    test_operator = SparkSubmitSHHOperator(
        app_file_name="test.py",
        task_id="test_operator",
        ssh_conn_id="ssh_test",
        py_files=["path.py"],
        dataset="abfs://raw/haaaaaaaaaallllllooooooo/nein.zip",
        connections=["azure_data_lake"],
        conn_env_mapping={"ADLS_STORAGE_ACCOUNT_NAME": "AZURE_DATA_LAKE__HOST"},
    )

    test_operator


dag_object = test_spark()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
