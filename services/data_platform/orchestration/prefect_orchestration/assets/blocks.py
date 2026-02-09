from prefect.blocks.system import Secret
from prefect_aws.s3 import AwsCredentials
from prefect_sqlalchemy import SqlAlchemyConnector

TRINO__HIVE_WAREHOUSE = "trino-hive"
TRINO__ICEBERG_WAREHOUSE = "trino-iceberg"
AWS__S3_LAKEHOUSE = "aws-credentials"
EOD__API_KEY = "eod-api-key"


def trino_hive_warehouse():
    """Load Trino Hive Warehouse from Prefect Secret."""

    database_block = SqlAlchemyConnector.load(TRINO__HIVE_WAREHOUSE)
    with database_block:
        return database_block.get_engine()


def trino_iceberg_warehouse():
    """Load Trino Iceberg Warehouse from Prefect Secret."""

    database_block = SqlAlchemyConnector.load(TRINO__ICEBERG_WAREHOUSE)
    with database_block:
        return database_block.get_engine()


def aws_s3_lakehouse():
    """Load AWS S3 Lakehouse from Prefect Secret."""

    aws_credentials = AwsCredentials.load("aws-credentials")
    return aws_credentials


def eod_api_key() -> str:
    """Load EOD API Key from Prefect Secret."""

    eod_api_key_block = Secret.load("eod-api-key")
    return eod_api_key_block.get()
