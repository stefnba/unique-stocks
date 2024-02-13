# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

import polars as pl
from airflow.decorators import dag, task
from conf.spark import config as spark_config
from conf.spark import packages as spark_packages
from custom.providers.spark.operators.submit import SparkSubmitSHHOperator
from shared import airflow_dataset
from shared import connections as CONN
from utils.dag.xcom import XComGetter


def transform_entity(data: pl.LazyFrame) -> pl.LazyFrame:
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


URL = "https://leidata-preview.gleif.org/storage/golden-copy-files/2023/07/17/808968/20230717-0000-gleif-goldencopy-lei2-golden-copy.csv.zip"


@task
def ingest():
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from custom.hooks.http.hook import HttpHook
    from shared.path import S3RawZonePath
    from utils.file.unzip import compress_with_gzip, unzip_file

    # download to file
    local_file = HttpHook().stream_to_local_file(url=URL, file_type="zip")

    # rezip
    unzipped_file_path = unzip_file(local_file)
    rezipped_path = compress_with_gzip(unzipped_file_path.uri)

    # ingest to data lake raw zone
    ingest_path = S3RawZonePath(source="Gleif", type="csv.gz", product="entity")
    hook = S3Hook(aws_conn_id=CONN.AWS_DATA_LAKE)
    hook.load_file(filename=rezipped_path, key=ingest_path.key, bucket_name=ingest_path.bucket)

    # todo remove file rezipped_path

    return ingest_path.uri


sink = SparkSubmitSHHOperator(
    task_id="sink_to_iceberg",
    app_file_name="sink_entity.py",
    ssh_conn_id="ssh_test",
    spark_conf={
        **spark_config.aws,
        **spark_config.iceberg_jdbc_catalog,
    },
    spark_packages=[*spark_packages.aws, *spark_packages.iceberg],
    connections=[CONN.AWS_DATA_LAKE, CONN.ICEBERG_CATALOG],
    dataset=XComGetter.pull_with_template("ingest"),
    conn_env_mapping={
        "AWS_ACCESS_KEY_ID": "AWS__LOGIN",
        "AWS_SECRET_ACCESS_KEY": "AWS__PASSWORD",
        "AWS_REGION": "AWS__EXTRA__REGION_NAME",
    },
    outlets=[airflow_dataset.Entity],
)


@dag(
    schedule="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["entity"],
)
def entity():
    ingest_task = ingest()
    ingest_task >> sink


dag_object = entity()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
