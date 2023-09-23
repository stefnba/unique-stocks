# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag


from custom.operators.data.transformation import DuckDbTransformationOperator, DataBindingCustomHandler
from custom.providers.azure.hooks.handlers.read import LocalDatasetReadHandler
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from shared.data_lake_path import EntityIsinPath, TempFile
from utils.dag.xcom import get_xcom_template
from shared import schema


@task
def ingest():
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook

    destination = EntityIsinPath.raw(source="Gleif", format="zip")
    url = "https://mapping.gleif.org/api/v2/isin-lei/d6996d23-cdaf-413e-b594-5219d40f3da5/download"

    hook = AzureDataLakeStorageHook(conn_id="azure_data_lake")
    hook.upload_from_url(url=url, container=destination.container, blob_path=destination.path)

    return destination.serialized


@task
def unizp(path):
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
    from utils.filesystem.path import TempFilePath
    from utils.file.unzip import unzip_file

    hook = AzureDataLakeStorageHook(conn_id="azure_data_lake")

    blob_path = path.get("path")
    container = path.get("container")
    file_path = TempFilePath.create(file_format="zip")

    # download to file
    hook.stream_to_local_file(container=container, blob_path=blob_path, file_path=file_path)

    # unzip
    unzipped_file_path = unzip_file(file_path)

    destination = TempFile(format="csv")

    # upload unzip file to azure
    hook.upload_file(
        container=destination.container, blob_path=destination.path, file_path=unzipped_file_path, stream=True
    )

    return destination.serialized


transform = DuckDbTransformationOperator(
    task_id="transform",
    adls_conn_id="azure_data_lake",
    destination_path=TempFile(),
    query="sql/entity_isin/transform.sql",
    data={
        "data": get_xcom_template(task_id="unizp"),
    },
)


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    adls_conn_id="azure_data_lake",
    dataset_path=get_xcom_template(task_id="transform"),
    destination_path=EntityIsinPath.curated(),
    pyarrow_options={
        "schema": schema.EntityIsin,
    },
    delta_table_options={
        "mode": "overwrite",
    },
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["entity"],
)
def entity_isin():
    ingest_task = ingest()
    unizp(ingest_task) >> transform >> sink


dag_object = entity_isin()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
