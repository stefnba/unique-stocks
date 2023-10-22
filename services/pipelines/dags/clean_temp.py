# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import dag, task


@task
def clean():
    """Delete all blobs in temp container."""

    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook

    hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")

    hook.delete_blobs(container="temp", start_with="")


@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["temp", "cleanup"],
    description="Delete and re-create temp container on Azure Data Lake Storage.",
)
def clean_temp():
    clean()


dag_object = clean_temp()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
