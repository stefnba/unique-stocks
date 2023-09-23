# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag

from utils.filesystem.path import TempFilePath

from custom.operators.data.transformation import (
    LazyFrameTransformationOperator,
)
from custom.providers.azure.hooks.handlers.read import AzureDatasetStreamHandler, AzureDatasetArrowHandler
from custom.providers.azure.hooks.handlers.write import LocalDatasetWriteHandler, AzureDatasetWriteUploadHandler
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator
from shared.data_lake_path import EntityPath, TempFile
from utils.dag.xcom import get_xcom_template
import polars as pl
from shared import schema


def transform_entity_job(data: pl.LazyFrame) -> pl.LazyFrame:
    data = data.select(
        [
            "LEI",
            "Entity.LegalName",
            "Entity.LegalForm.EntityLegalFormCode",
            "Entity.LegalJurisdiction",
            "Entity.LegalAddress.FirstAddressLine",
            "Entity.LegalAddress.AddressNumber",
            "Entity.LegalAddress.PostalCode",
            "Entity.LegalAddress.City",
            "Entity.LegalAddress.Country",
            "Entity.HeadquartersAddress.FirstAddressLine",
            "Entity.HeadquartersAddress.AddressNumber",
            "Entity.HeadquartersAddress.City",
            "Entity.HeadquartersAddress.PostalCode",
            "Entity.HeadquartersAddress.Country",
            "Entity.EntityStatus",
            "Entity.EntityCreationDate",
            "Entity.EntityExpirationDate",
            "Entity.EntityExpirationReason",
            "Registration.InitialRegistrationDate",
            "Registration.RegistrationStatus",
        ]
    )

    # data = data.head(500_000)

    data = data.rename(
        {
            "LEI": "lei",
            "Entity.LegalName": "name",
            "Entity.LegalForm.EntityLegalFormCode": "legal_form_id",
            "Entity.LegalJurisdiction": "jurisdiction",
            "Entity.LegalAddress.FirstAddressLine": "legal_address_street",
            "Entity.LegalAddress.AddressNumber": "legal_address_street_number",
            "Entity.LegalAddress.PostalCode": "legal_address_zip_code",
            "Entity.LegalAddress.City": "legal_address_city",
            "Entity.LegalAddress.Country": "legal_address_country",
            "Entity.HeadquartersAddress.FirstAddressLine": "headquarter_address_street",
            "Entity.HeadquartersAddress.AddressNumber": "headquarter_address_street_number",
            "Entity.HeadquartersAddress.City": "headquarter_address_city",
            "Entity.HeadquartersAddress.PostalCode": "headquarter_address_zip_code",
            "Entity.HeadquartersAddress.Country": "headquarter_address_country",
            "Entity.EntityStatus": "status",
            "Entity.EntityCreationDate": "creation_date",
            "Entity.EntityExpirationDate": "expiration_date",
            "Entity.EntityExpirationReason": "expiration_reason",
            "Registration.InitialRegistrationDate": "registration_date",
            "Registration.RegistrationStatus": "registration_status",
        }
    )

    return data


@task
def ingest():
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook

    destination = EntityPath.raw(source="Gleif", format="zip")

    url = "https://leidata-preview.gleif.org/storage/golden-copy-files/2023/07/17/808968/20230717-0000-gleif-goldencopy-lei2-golden-copy.csv.zip"

    hook = AzureDataLakeStorageHook(conn_id="azure_data_lake")
    hook.upload_from_url(url=url, container=destination.container, blob_path=destination.path)

    return destination.serialized


@task
def unizp_transform(path):
    from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
    from utils.file.unzip import unzip_file

    hook = AzureDataLakeStorageHook(conn_id="azure_data_lake")

    blob_path = path.get("path")
    container = path.get("container")
    file_path = TempFilePath.create(file_format="zip")

    # download to file
    hook.stream_to_local_file(container=container, blob_path=blob_path, file_path=file_path)

    # unzip
    unzipped_file_path = unzip_file(file_path)

    lf = pl.scan_csv(unzipped_file_path)

    transformed = transform_entity_job(lf)

    file_path_parquet = TempFilePath.create()
    transformed.sink_parquet(file_path_parquet)

    # pl.scan_csv(unzipped_file_path).sink_parquet(file_path_parquet)

    destination = TempFile()

    # upload unzip file to azure
    hook.upload_file(
        container=destination.container, blob_path=destination.path, file_path=file_path_parquet, stream=True
    )

    return destination.serialized


# transform = LazyFrameTransformationOperator(
#     task_id="transform",
#     adls_conn_id="azure_data_lake",
#     destination_path=TempFilePath.create(),
#     dataset_format="csv",
#     dataset_path=get_xcom_template(task_id="unizp"),
#     dataset_handler=AzureDatasetStreamHandler,
#     write_handler=AzureDatasetWriteUploadHandler,
#     transformation=transform_entity_job,
# )


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    adls_conn_id="azure_data_lake",
    dataset_path=get_xcom_template(task_id="unizp_transform"),
    dataset_handler=AzureDatasetArrowHandler,
    destination_path=EntityPath.curated(),
    pyarrow_options={
        "schema": schema.Entity,
    },
    delta_table_options={
        "mode": "overwrite",
        # "partition_by": ["headquarter_address_country"],
    },
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["entity"],
)
def entity():
    ingest_task = ingest()
    unizp_transform(ingest_task) >> sink


dag_object = entity()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)