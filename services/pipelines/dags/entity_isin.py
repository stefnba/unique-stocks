# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import dag, task
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from custom.operators.data.transformation import DuckDbTransformationOperator
from shared import airflow_dataset, schema
from shared.path import AdlsPath, EntityIsinPath, LocalPath
from utils.dag.xcom import XComGetter

URL = "https://mapping.gleif.org/api/v2/isin-lei/d6996d23-cdaf-413e-b594-5219d40f3da5/download"


@task
def ingest():
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook

    destination = EntityIsinPath.raw(source="Gleif", format="zip")

    hook = AzureDataLakeStorageHook(conn_id="azure_data_lake")
    hook.upload_from_url(url=URL, **destination.afls_path)

    return destination.to_dict()


@task
def unizp(path):
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
    from utils.file.unzip import unzip_file

    hook = AzureDataLakeStorageHook(conn_id="azure_data_lake")

    path = AdlsPath.create(path)

    file_path = LocalPath.create_temp_file_path(format="zip")

    # download to file
    hook.stream_to_local_file(**path.afls_path, file_path=file_path)

    # unzip
    unzipped_file_path = unzip_file(file_path)

    destination = AdlsPath.create_temp_file_path(format="csv")

    # upload unzip file to azure
    hook.upload_file(**destination.afls_path, file_path=unzipped_file_path, stream=True)

    return destination.to_dict()


transform = DuckDbTransformationOperator(
    task_id="transform",
    adls_conn_id="azure_data_lake",
    destination_path=AdlsPath.create_temp_file_path(),
    query="sql/entity_isin/transform.sql",
    data={
        "data": XComGetter.pull_with_template(task_id="unizp"),
    },
)


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    adls_conn_id="azure_data_lake",
    dataset_path=XComGetter.pull_with_template(task_id="transform"),
    destination_path=EntityIsinPath.curated(),
    pyarrow_options={
        "schema": schema.EntityIsin,
    },
    delta_table_options={
        "mode": "overwrite",
    },
)


@dag(
    schedule=[airflow_dataset.Entity],
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
