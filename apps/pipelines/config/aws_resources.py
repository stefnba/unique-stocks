"""Non-secret AWS resource base defaults for the pipelines app."""

DEFAULT_REGION = "eu-central-1"
DEFAULT_BUCKET_BASE_NAME = "unique-stocks"

DEFAULT_IAM_USER_BASE_NAME = "unique-stocks-pipelines"
DEFAULT_INLINE_POLICY_BASE_NAME = "unique-stocks-pipelines-s3-landing"

__all__ = [
    "DEFAULT_BUCKET_BASE_NAME",
    "DEFAULT_IAM_USER_BASE_NAME",
    "DEFAULT_INLINE_POLICY_BASE_NAME",
    "DEFAULT_REGION",
]
