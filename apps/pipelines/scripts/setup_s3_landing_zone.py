"""Provision the AWS S3 landing zone used by the ingestion pipeline.

Run from ``apps/pipelines/``.  All commands below assume that working directory.

Preview the provisioning plan without calling AWS::

    uv run python scripts/setup_s3_landing_zone.py --profile provisioner --dry-run

Create or update the bucket, security controls, IAM user, and inline policy::

    uv run python scripts/setup_s3_landing_zone.py --profile provisioner

Access key creation is a separate, deliberate step. Run without ``--create-access-key``
first to provision the bucket and IAM user, verify the output, then re-run with the
flag only when you are ready to immediately store the secret — AWS shows
``SecretAccessKey`` only once::

    # Step 1 — provision everything except the access key (idempotent, safe to re-run)
    uv run python scripts/setup_s3_landing_zone.py --profile provisioner

    # Step 2 — create the access key only when ready to store it
    uv run python scripts/setup_s3_landing_zone.py --profile provisioner --create-access-key

Grant ``s3:DeleteObject`` only when a cleanup workflow explicitly requires it::

    uv run python scripts/setup_s3_landing_zone.py --profile provisioner --allow-delete

Target a different bucket, region, or IAM user (e.g. for the prod environment)::

    uv run python scripts/setup_s3_landing_zone.py \\
        --profile provisioner \\
        --bucket unique-stocks-prod \\
        --region eu-central-1 \\
        --user unique-stocks-prod-pipelines

The same targets are available through Make::

    make s3-landing-zone ARGS="--profile provisioner --dry-run"

After a successful run, store the returned ``AccessKeyId`` and ``SecretAccessKey``
in ``.env`` or the production secret store, then persist the Prefect blocks::

    make blocks-save
"""

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    ProfileNotFound,
)
from pydantic import BaseModel, ConfigDict, Field

# Allow `from config.blocks import …` to resolve when the script is run directly.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

TLS_POLICY_SID = "DenyNonTLS"


class AwsCallerIdentity(BaseModel):
    """Typed subset of the STS GetCallerIdentity response."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    user_id: str = Field(alias="UserId")
    account: str = Field(alias="Account")
    arn: str = Field(alias="Arn")


class AccessKeyMetadata(BaseModel):
    """Typed subset of IAM access key metadata."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    access_key_id: str = Field(alias="AccessKeyId")
    status: str = Field(alias="Status")


class AccessKeySecret(BaseModel):
    """Typed subset of IAM CreateAccessKey response."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    user_name: str = Field(alias="UserName")
    access_key_id: str = Field(alias="AccessKeyId")
    secret_access_key: str = Field(alias="SecretAccessKey")
    status: str = Field(alias="Status")


class BucketPolicyResponse(BaseModel):
    """Typed subset of the S3 GetBucketPolicy response."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    policy: str = Field(alias="Policy")


@dataclass(frozen=True, slots=True)
class LandingZoneConfig:
    """Resolved provisioning configuration."""

    bucket_name: str
    region: str
    iam_user: str
    policy_name: str
    profile: str | None
    create_access_key: bool
    allow_delete: bool
    dry_run: bool


@dataclass(frozen=True, slots=True)
class AwsResourceDefaults:
    """Non-secret AWS resource defaults used by the provisioning script."""

    bucket_name: str
    region: str
    iam_user: str
    inline_policy_name: str


class ProvisioningError(RuntimeError):
    """Raised when provisioning cannot safely continue."""


def emit(message: str = "") -> None:
    """Write CLI output."""
    print(message)


def selected(data: Mapping[str, Any], keys: Sequence[str]) -> dict[str, Any]:
    """Return selected keys from a boto3 response for strict Pydantic validation."""
    try:
        return {key: data[key] for key in keys}
    except KeyError as exc:
        raise ProvisioningError(f"Unexpected AWS response shape, missing key: {exc}") from exc


def bool_label(value: bool) -> str:
    """Return a lowercase shell-style boolean label."""
    return "true" if value else "false"


def aws_resource_defaults() -> AwsResourceDefaults:
    """Load non-secret AWS resource defaults without importing Prefect block wiring."""
    from config.aws_resources import (
        DEFAULT_BUCKET_NAME,
        DEFAULT_IAM_USER,
        DEFAULT_INLINE_POLICY_NAME,
        DEFAULT_REGION,
    )

    return AwsResourceDefaults(
        bucket_name=DEFAULT_BUCKET_NAME,
        region=DEFAULT_REGION,
        iam_user=DEFAULT_IAM_USER,
        inline_policy_name=DEFAULT_INLINE_POLICY_NAME,
    )


def parse_args(argv: Sequence[str] | None = None) -> LandingZoneConfig:
    """Parse command-line arguments into a provisioning config."""
    defaults = aws_resource_defaults()
    parser = argparse.ArgumentParser(
        description="Set up the S3 landing-zone bucket and IAM user for the pipelines app.",
    )
    parser.add_argument(
        "--bucket",
        default=defaults.bucket_name,
        help=f"S3 bucket name. Default: {defaults.bucket_name}",
    )
    parser.add_argument("--region", default=defaults.region, help=f"AWS region. Default: {defaults.region}")
    parser.add_argument("--user", default=defaults.iam_user, help=f"IAM user name. Default: {defaults.iam_user}")
    parser.add_argument(
        "--policy-name",
        default=defaults.inline_policy_name,
        help=f"Inline IAM policy name. Default: {defaults.inline_policy_name}",
    )
    parser.add_argument("--profile", default=None, help="AWS profile name to use.")
    parser.add_argument(
        "--create-access-key",
        action="store_true",
        help="Create and print a new access key for the IAM user.",
    )
    parser.add_argument(
        "--allow-delete",
        action="store_true",
        help="Grant s3:DeleteObject in the IAM policy. Off by default.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print planned actions without calling AWS.")

    args = parser.parse_args(argv)
    config = LandingZoneConfig(
        bucket_name=args.bucket,
        region=args.region,
        iam_user=args.user,
        policy_name=args.policy_name,
        profile=args.profile,
        create_access_key=args.create_access_key,
        allow_delete=args.allow_delete,
        dry_run=args.dry_run,
    )
    validate_config(config)
    return config


def validate_config(config: LandingZoneConfig) -> None:
    """Validate non-empty CLI values."""
    if not config.bucket_name:
        raise ProvisioningError("Bucket name cannot be empty.")
    if not config.region:
        raise ProvisioningError("Region cannot be empty.")
    if not config.iam_user:
        raise ProvisioningError("IAM user cannot be empty.")
    if not config.policy_name:
        raise ProvisioningError("Policy name cannot be empty.")


def create_session(config: LandingZoneConfig) -> boto3.Session:
    """Create a boto3 session from the resolved config."""
    return boto3.Session(profile_name=config.profile, region_name=config.region)


def planned(config: LandingZoneConfig, action: str, payload: Mapping[str, Any] | None = None) -> None:
    """Print a dry-run action."""
    emit(f"DRY RUN: {action}")
    if payload:
        emit(json.dumps(payload, indent=2, sort_keys=True))


def landing_policy(config: LandingZoneConfig) -> dict[str, Any]:
    """Build the least-privilege inline IAM policy for the landing-zone user."""
    object_actions = [
        "s3:AbortMultipartUpload",
        "s3:GetObject",
        "s3:ListMultipartUploadParts",
        "s3:PutObject",
    ]
    if config.allow_delete:
        object_actions.insert(1, "s3:DeleteObject")

    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ListLandingBucket",
                "Effect": "Allow",
                "Action": [
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                ],
                "Resource": f"arn:aws:s3:::{config.bucket_name}",
            },
            {
                "Sid": "ReadWriteLandingObjects",
                "Effect": "Allow",
                "Action": object_actions,
                "Resource": f"arn:aws:s3:::{config.bucket_name}/*",
            },
        ],
    }


def tls_bucket_policy_statement(bucket_name: str) -> dict[str, Any]:
    """Build a bucket policy statement that denies all non-TLS (HTTP) requests."""
    return {
        "Sid": TLS_POLICY_SID,
        "Effect": "Deny",
        "Principal": "*",
        "Action": "s3:*",
        "Resource": [
            f"arn:aws:s3:::{bucket_name}",
            f"arn:aws:s3:::{bucket_name}/*",
        ],
        "Condition": {"Bool": {"aws:SecureTransport": "false"}},
    }


def tls_bucket_policy(bucket_name: str) -> dict[str, Any]:
    """Build a bucket policy that only contains the non-TLS deny statement."""
    return {
        "Version": "2012-10-17",
        "Statement": [tls_bucket_policy_statement(bucket_name)],
    }


def bucket_policy_statements(policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return bucket policy statements in list form."""
    statements = policy.get("Statement", [])
    if isinstance(statements, Mapping):
        statements = [statements]
    if not isinstance(statements, list):
        raise ProvisioningError("Unexpected S3 bucket policy shape: Statement must be an object or list.")

    normalized: list[dict[str, Any]] = []
    for statement in statements:
        if not isinstance(statement, Mapping):
            raise ProvisioningError("Unexpected S3 bucket policy shape: every Statement item must be an object.")
        normalized.append(dict(statement))
    return normalized


def get_bucket_policy(s3_client: Any, bucket_name: str) -> dict[str, Any] | None:
    """Fetch the current bucket policy, if one exists."""
    try:
        response = s3_client.get_bucket_policy(Bucket=bucket_name)
    except ClientError as exc:
        error = exc.response.get("Error", {})
        if error.get("Code") == "NoSuchBucketPolicy":
            return None
        raise

    bucket_policy_response = BucketPolicyResponse.model_validate(selected(response, ["Policy"]))

    try:
        policy = json.loads(bucket_policy_response.policy)
    except json.JSONDecodeError as exc:
        raise ProvisioningError("Unexpected AWS response shape: S3 bucket policy was not valid JSON.") from exc

    if not isinstance(policy, dict):
        raise ProvisioningError("Unexpected S3 bucket policy shape: policy root must be an object.")
    return policy


def merge_tls_bucket_policy(existing_policy: Mapping[str, Any] | None, bucket_name: str) -> dict[str, Any]:
    """Return a bucket policy with the non-TLS deny statement upserted."""
    if existing_policy is None:
        return tls_bucket_policy(bucket_name)

    merged_policy = dict(existing_policy)
    merged_policy["Version"] = existing_policy.get("Version", "2012-10-17")
    merged_policy["Statement"] = [
        statement for statement in bucket_policy_statements(existing_policy) if statement.get("Sid") != TLS_POLICY_SID
    ]
    merged_policy["Statement"].append(tls_bucket_policy_statement(bucket_name))
    return merged_policy


def bucket_exists(s3_client: Any, bucket_name: str) -> bool:
    """Return whether the bucket exists and is accessible to this caller."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = str(error.get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"404", "NoSuchBucket", "NotFound"} or status == 404:
            return False
        if code in {"403", "AccessDenied"} or status == 403:
            raise ProvisioningError(
                f"Bucket {bucket_name!r} exists but is not accessible to this AWS principal."
            ) from exc
        raise
    return True


def ensure_bucket(s3_client: Any, config: LandingZoneConfig) -> None:
    """Create the S3 bucket if it does not already exist."""
    payload: dict[str, Any] = {"Bucket": config.bucket_name}
    if config.region != "us-east-1":
        payload["CreateBucketConfiguration"] = {"LocationConstraint": config.region}

    if config.dry_run:
        planned(config, "create bucket if missing", payload)
        return

    if bucket_exists(s3_client, config.bucket_name):
        emit(f"Bucket already exists: {config.bucket_name}")
        return

    s3_client.create_bucket(**payload)
    emit(f"Created bucket: {config.bucket_name}")


def apply_bucket_security(s3_client: Any, config: LandingZoneConfig) -> None:
    """Apply baseline S3 security controls to the landing-zone bucket."""
    public_access = {
        "BlockPublicAcls": True,
        "IgnorePublicAcls": True,
        "BlockPublicPolicy": True,
        "RestrictPublicBuckets": True,
    }
    ownership_controls = {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]}
    encryption = {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}
    versioning = {"Status": "Enabled"}

    if config.dry_run:
        planned(config, "put public access block", public_access)
        planned(config, "put bucket ownership controls", ownership_controls)
        planned(config, "put bucket encryption", encryption)
        planned(config, "put bucket versioning", versioning)
        return

    s3_client.put_public_access_block(
        Bucket=config.bucket_name,
        PublicAccessBlockConfiguration=public_access,
    )
    s3_client.put_bucket_ownership_controls(
        Bucket=config.bucket_name,
        OwnershipControls=ownership_controls,
    )
    s3_client.put_bucket_encryption(
        Bucket=config.bucket_name,
        ServerSideEncryptionConfiguration=encryption,
    )
    s3_client.put_bucket_versioning(
        Bucket=config.bucket_name,
        VersioningConfiguration=versioning,
    )
    emit("Applied bucket security controls.")


def apply_tls_bucket_policy(s3_client: Any, config: LandingZoneConfig) -> None:
    """Enforce HTTPS-only access by upserting a Deny-non-TLS bucket policy statement."""
    if config.dry_run:
        planned(config, "upsert TLS-enforce bucket policy statement", tls_bucket_policy(config.bucket_name))
        return

    policy = merge_tls_bucket_policy(get_bucket_policy(s3_client, config.bucket_name), config.bucket_name)
    s3_client.put_bucket_policy(
        Bucket=config.bucket_name,
        Policy=json.dumps(policy, separators=(",", ":")),
    )
    emit("Applied TLS-enforce bucket policy statement.")


def iam_user_exists(iam_client: Any, user_name: str) -> bool:
    """Return whether the IAM user exists."""
    try:
        iam_client.get_user(UserName=user_name)
    except ClientError as exc:
        error = exc.response.get("Error", {})
        if error.get("Code") == "NoSuchEntity":
            return False
        raise
    return True


def ensure_iam_user(iam_client: Any, config: LandingZoneConfig) -> None:
    """Create the IAM user if it does not already exist."""
    payload = {"UserName": config.iam_user}

    if config.dry_run:
        planned(config, "create IAM user if missing", payload)
        return

    if iam_user_exists(iam_client, config.iam_user):
        emit(f"IAM user already exists: {config.iam_user}")
        return

    iam_client.create_user(**payload)
    emit(f"Created IAM user: {config.iam_user}")


def put_user_policy(iam_client: Any, config: LandingZoneConfig) -> None:
    """Attach or replace the bucket-scoped inline IAM policy."""
    policy = landing_policy(config)
    payload = {
        "UserName": config.iam_user,
        "PolicyName": config.policy_name,
        "PolicyDocument": policy,
    }

    if config.dry_run:
        planned(config, "put inline IAM user policy", payload)
        return

    iam_client.put_user_policy(
        UserName=config.iam_user,
        PolicyName=config.policy_name,
        PolicyDocument=json.dumps(policy, separators=(",", ":")),
    )
    emit(f"Attached inline IAM policy: {config.policy_name}")


def access_key_count(iam_client: Any, user_name: str) -> int:
    """Return the number of active or inactive access keys attached to the IAM user."""
    response = iam_client.list_access_keys(UserName=user_name)
    metadata = response.get("AccessKeyMetadata", [])
    validated = [AccessKeyMetadata.model_validate(selected(item, ["AccessKeyId", "Status"])) for item in metadata]
    return len(validated)


def maybe_create_access_key(iam_client: Any, config: LandingZoneConfig) -> None:
    """Create an access key only when explicitly requested."""
    if not config.create_access_key:
        emit(f"Access key creation skipped. Re-run with --create-access-key when ready for {config.iam_user}.")
        return

    payload = {"UserName": config.iam_user}
    if config.dry_run:
        planned(config, "create IAM access key", payload)
        return

    key_count = access_key_count(iam_client, config.iam_user)
    if key_count >= 2:
        raise ProvisioningError(
            f"IAM user {config.iam_user!r} already has {key_count} access keys. "
            "Delete or deactivate an old key before creating another."
        )

    response = iam_client.create_access_key(**payload)
    access_key = AccessKeySecret.model_validate(
        selected(response["AccessKey"], ["UserName", "AccessKeyId", "SecretAccessKey", "Status"])
    )
    emit("Created access key. AWS shows SecretAccessKey only once; store it now.")
    emit(json.dumps(access_key.model_dump(by_alias=True), indent=2))


def print_header(config: LandingZoneConfig) -> None:
    """Print resolved configuration before provisioning."""
    emit("S3 landing-zone setup")
    emit(f"  bucket:       {config.bucket_name}")
    emit(f"  region:       {config.region}")
    emit(f"  iam user:     {config.iam_user}")
    emit(f"  policy name:  {config.policy_name}")
    emit(f"  profile:      {config.profile or 'default'}")
    emit(f"  allow delete: {bool_label(config.allow_delete)}")
    emit(f"  dry run:      {bool_label(config.dry_run)}")


def verify_identity(sts_client: Any, config: LandingZoneConfig) -> None:
    """Print the AWS caller identity for non-dry-run executions."""
    if config.dry_run:
        return

    response = sts_client.get_caller_identity()
    identity = AwsCallerIdentity.model_validate(selected(response, ["UserId", "Account", "Arn"]))
    emit(f"AWS caller: {identity.arn} (account {identity.account})")


def provision(config: LandingZoneConfig) -> None:
    """Provision the landing-zone bucket, IAM user, policy, and optional access key."""
    print_header(config)

    if config.dry_run:
        s3_client = None
        iam_client = None
        sts_client = None
    else:
        session = create_session(config)
        s3_client = session.client("s3")
        iam_client = session.client("iam")
        sts_client = session.client("sts")

    verify_identity(sts_client, config)
    ensure_bucket(s3_client, config)
    apply_bucket_security(s3_client, config)
    apply_tls_bucket_policy(s3_client, config)
    ensure_iam_user(iam_client, config)
    put_user_policy(iam_client, config)
    maybe_create_access_key(iam_client, config)

    emit("Done.")
    emit("Next: put AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env or production secrets,")
    emit("then run: make blocks-save")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI."""
    try:
        provision(parse_args(argv))
    except (ProvisioningError, NoCredentialsError, ProfileNotFound) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = error.get("Code", "Unknown")
        message = error.get("Message", str(exc))
        print(f"AWS error ({code}): {message}", file=sys.stderr)
        return 1
    except BotoCoreError as exc:
        print(f"AWS client error: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
