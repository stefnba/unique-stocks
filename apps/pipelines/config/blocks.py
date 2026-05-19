"""Prefect block registry for the pipelines app.

Each entry pairs a Prefect registry name with a typed block instance built
from application settings.  Use ``BlockRegistry.<NAME>.load()`` or
``BlockRegistry.<NAME>.load_async()`` in tasks to retrieve the live block
value from Prefect at runtime.
"""

from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials

from config.settings import SETTINGS
from core.blocks import define_block


class BlockRegistry:
    """Central registry of all named Prefect blocks used by this app."""

    EODHD_API_KEY = define_block(
        "eodhd-api-key",
        Secret(value=SETTINGS.eodhd_api_key),
    )
    AWS_CREDENTIALS = define_block(
        "aws-credentials",
        AwsCredentials(
            aws_access_key_id=SETTINGS.aws_access_key_id,
            aws_secret_access_key=SETTINGS.aws_secret_access_key,
        ),
    )
