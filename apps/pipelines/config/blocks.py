"""Prefect block registry for the pipelines app.

Import ``BlockRegistry`` in tasks and flows to load blocks at runtime::

    api_key = await BlockRegistry.EODHD_API_KEY.load_async()

Run this file directly to register (or overwrite) all blocks on the Prefect
server::

    uv run python -m config.blocks
"""

from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials, S3Bucket

from config.settings import SETTINGS
from core.blocks import BlockRegistryBase, define_block


class BlockRegistry(BlockRegistryBase):
    """Central registry of all named Prefect blocks used by this app."""

    EODHD_API_KEY = define_block(
        "eodhd-api-key",
        Secret(value=SETTINGS.eodhd_api_key),
    )
    S3_BUCKET = define_block(
        "s3-bucket",
        S3Bucket(
            bucket_name=SETTINGS.s3_bucket or "",
            credentials=AwsCredentials(
                aws_access_key_id=SETTINGS.aws_access_key_id,
                aws_secret_access_key=SETTINGS.aws_secret_access_key,
                region_name=SETTINGS.aws_region,
            ),
        ),
    )


if __name__ == "__main__":
    """Run this file directly to register (or overwrite) all blocks on the Prefect server."""
    BlockRegistry.save_all(if_exists="overwrite")
