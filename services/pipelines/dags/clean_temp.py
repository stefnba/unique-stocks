# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime
from airflow.decorators import task, dag


@task
def clean():
    """Delete and re-create temp container on Azure Data Lake Storage."""
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
    import time
    from azure.core.exceptions import ResourceNotFoundError
    import logging

    hook = AzureDataLakeStorageHook(conn_id="azure_data_lake")

    try:
        hook.remove_container(container="temp")
        logging.info("Container removed.")
        # After you delete a container, you can't create a container with the same name for at least 30 seconds.
        # Attempting to create a container with the same name will fail with HTTP error code 409 (Conflict). Any other
        # operations on the container or the blobs it contains will fail with HTTP error code 404 (Not Found).
        time.sleep(60)
    except ResourceNotFoundError:
        pass

    hook.create_container(container="temp")
    logging.info("Container created.")


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
