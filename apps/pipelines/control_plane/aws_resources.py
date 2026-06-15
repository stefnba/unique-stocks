"""Application AWS control-plane resource-name wiring."""

from config.settings import get_settings
from core.storage.s3.provisioning import AwsResourceDefaults

AWS_REGION = "eu-central-1"

DEV_AWS_RESOURCES = AwsResourceDefaults(
    bucket_name="unique-stocks-dev",
    region=AWS_REGION,
    iam_user="unique-stocks-pipelines-dev",
    inline_policy_name="unique-stocks-pipelines-s3-landing-dev",
)

PROD_AWS_RESOURCES = AwsResourceDefaults(
    bucket_name="unique-stocks-prod",
    region=AWS_REGION,
    iam_user="unique-stocks-pipelines-prod",
    inline_policy_name="unique-stocks-pipelines-s3-landing-prod",
)


def aws_resource_defaults() -> AwsResourceDefaults:
    """Return concrete AWS resource defaults for the active app environment."""
    return PROD_AWS_RESOURCES if get_settings().is_production else DEV_AWS_RESOURCES


AWS_RESOURCES = aws_resource_defaults()


__all__ = ["AWS_RESOURCES", "aws_resource_defaults"]
