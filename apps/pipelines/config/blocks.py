"""Prefect block registry for the pipelines app.

Import ``BlockRegistry`` in tasks and flows to load blocks at runtime::

    api_key = await BlockRegistry.EODHD_API_KEY.load_async()

Run via make to register (or overwrite) all blocks on the Prefect server::

    make blocks-save
"""

from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials, S3Bucket

from config.settings import SETTINGS
from core.blocks import BlockRegistryBase, define_block

DEFAULT_REGION = "eu-central-1"
DEFAULT_BUCKET_NAME = "unique-stocks-dev"


aws_credentials = AwsCredentials(
    aws_access_key_id=SETTINGS.aws_access_key_id,
    aws_secret_access_key=SETTINGS.aws_secret_access_key,
    region_name=DEFAULT_REGION,
)


class BlockRegistry(BlockRegistryBase):
    """Central registry of all named Prefect blocks used by this app."""

    EODHD_API_KEY = define_block(
        "eodhd-api-key",
        Secret(value=SETTINGS.eodhd_api_key),
    )

    AWS_CREDENTIALS = define_block("aws-credentials", aws_credentials)

    S3_BUCKET = define_block(
        "s3-bucket",
        S3Bucket(
            bucket_name=DEFAULT_BUCKET_NAME,
            credentials=aws_credentials,
        ),
    )


__all__ = ["BlockRegistry"]


if __name__ == "__main__":
    BlockRegistry.save_all(if_exists="overwrite")
