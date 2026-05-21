"""Non-secret AWS resource defaults for the pipelines app."""

from core.utils.env import by_env

# AWS region and bucket name
DEFAULT_REGION = "eu-central-1"
DEFAULT_BUCKET_NAME = by_env(default="unique-stocks", add_env="suffix")

# IAM user and policy names
DEFAULT_IAM_USER = by_env(default="unique-stocks-pipelines", add_env="suffix")
DEFAULT_INLINE_POLICY_NAME = by_env(default="unique-stocks-pipelines-s3-landing", add_env="suffix")


