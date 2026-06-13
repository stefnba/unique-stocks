r"""Provisioning primitives for the AWS S3 landing zone used by ingestion."""

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, ConfigDict, Field

TLS_POLICY_SID = "DenyNonTLS"
type Emit = Callable[["ProvisioningEvent"], None]


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


@dataclass(frozen=True, slots=True)
class ProvisioningEvent:
    """Structured progress event from landing-zone provisioning."""

    name: str
    payload: Mapping[str, Any] | None = None


class ProvisioningError(RuntimeError):
    """Raised when provisioning cannot safely continue."""


def _noop_emit(_event: ProvisioningEvent) -> None:
    """Ignore provisioning progress events."""


def selected(data: Mapping[str, Any], keys: Sequence[str]) -> dict[str, Any]:
    """Return selected keys from a boto3 response for strict Pydantic validation."""
    try:
        return {key: data[key] for key in keys}
    except KeyError as exc:
        raise ProvisioningError(f"Unexpected AWS response shape, missing key: {exc}") from exc


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


def planned(
    action: str,
    request: Mapping[str, Any] | None = None,
    *,
    emit: Emit,
) -> None:
    """Emit a dry-run action."""
    emit(ProvisioningEvent(name="dry_run", payload={"action": action, "request": request}))


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


def ensure_bucket(s3_client: Any, config: LandingZoneConfig, *, emit: Emit = _noop_emit) -> None:
    """Create the S3 bucket if it does not already exist."""
    payload: dict[str, Any] = {"Bucket": config.bucket_name}
    if config.region != "us-east-1":
        payload["CreateBucketConfiguration"] = {"LocationConstraint": config.region}

    if config.dry_run:
        planned("create_bucket_if_missing", payload, emit=emit)
        return

    if bucket_exists(s3_client, config.bucket_name):
        emit(ProvisioningEvent(name="bucket_exists", payload={"bucket_name": config.bucket_name}))
        return

    s3_client.create_bucket(**payload)
    emit(ProvisioningEvent(name="bucket_created", payload={"bucket_name": config.bucket_name}))


def apply_bucket_security(s3_client: Any, config: LandingZoneConfig, *, emit: Emit = _noop_emit) -> None:
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
        planned("put_public_access_block", public_access, emit=emit)
        planned("put_bucket_ownership_controls", ownership_controls, emit=emit)
        planned("put_bucket_encryption", encryption, emit=emit)
        planned("put_bucket_versioning", versioning, emit=emit)
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
    emit(ProvisioningEvent(name="bucket_security_applied"))


def apply_tls_bucket_policy(s3_client: Any, config: LandingZoneConfig, *, emit: Emit = _noop_emit) -> None:
    """Enforce HTTPS-only access by upserting a Deny-non-TLS bucket policy statement."""
    if config.dry_run:
        planned(
            "upsert_tls_bucket_policy_statement",
            tls_bucket_policy(config.bucket_name),
            emit=emit,
        )
        return

    policy = merge_tls_bucket_policy(get_bucket_policy(s3_client, config.bucket_name), config.bucket_name)
    s3_client.put_bucket_policy(
        Bucket=config.bucket_name,
        Policy=json.dumps(policy, separators=(",", ":")),
    )
    emit(ProvisioningEvent(name="tls_bucket_policy_applied"))


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


def ensure_iam_user(iam_client: Any, config: LandingZoneConfig, *, emit: Emit = _noop_emit) -> None:
    """Create the IAM user if it does not already exist."""
    payload = {"UserName": config.iam_user}

    if config.dry_run:
        planned("create_iam_user_if_missing", payload, emit=emit)
        return

    if iam_user_exists(iam_client, config.iam_user):
        emit(ProvisioningEvent(name="iam_user_exists", payload={"iam_user": config.iam_user}))
        return

    iam_client.create_user(**payload)
    emit(ProvisioningEvent(name="iam_user_created", payload={"iam_user": config.iam_user}))


def put_user_policy(iam_client: Any, config: LandingZoneConfig, *, emit: Emit = _noop_emit) -> None:
    """Attach or replace the bucket-scoped inline IAM policy."""
    policy = landing_policy(config)
    payload = {
        "UserName": config.iam_user,
        "PolicyName": config.policy_name,
        "PolicyDocument": policy,
    }

    if config.dry_run:
        planned("put_inline_iam_user_policy", payload, emit=emit)
        return

    iam_client.put_user_policy(
        UserName=config.iam_user,
        PolicyName=config.policy_name,
        PolicyDocument=json.dumps(policy, separators=(",", ":")),
    )
    emit(ProvisioningEvent(name="user_policy_attached", payload={"policy_name": config.policy_name}))


def access_key_count(iam_client: Any, user_name: str) -> int:
    """Return the number of active or inactive access keys attached to the IAM user."""
    response = iam_client.list_access_keys(UserName=user_name)
    metadata = response.get("AccessKeyMetadata", [])
    validated = [AccessKeyMetadata.model_validate(selected(item, ["AccessKeyId", "Status"])) for item in metadata]
    return len(validated)


def maybe_create_access_key(iam_client: Any, config: LandingZoneConfig, *, emit: Emit = _noop_emit) -> None:
    """Create an access key only when explicitly requested."""
    if not config.create_access_key:
        emit(ProvisioningEvent(name="access_key_skipped", payload={"iam_user": config.iam_user}))
        return

    payload = {"UserName": config.iam_user}
    if config.dry_run:
        planned("create_iam_access_key", payload, emit=emit)
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
    emit(ProvisioningEvent(name="access_key_created", payload=access_key.model_dump(by_alias=True)))


def verify_identity(sts_client: Any, config: LandingZoneConfig, *, emit: Emit = _noop_emit) -> None:
    """Print the AWS caller identity for non-dry-run executions."""
    if config.dry_run:
        return

    response = sts_client.get_caller_identity()
    identity = AwsCallerIdentity.model_validate(selected(response, ["UserId", "Account", "Arn"]))
    emit(
        ProvisioningEvent(
            name="aws_caller_identity",
            payload={"arn": identity.arn, "account": identity.account},
        )
    )


def provision(config: LandingZoneConfig, *, emit: Emit = _noop_emit) -> None:
    """Provision the landing-zone bucket, IAM user, policy, and optional access key."""
    if config.dry_run:
        s3_client = None
        iam_client = None
        sts_client = None
    else:
        session = create_session(config)
        s3_client = session.client("s3")
        iam_client = session.client("iam")
        sts_client = session.client("sts")

    verify_identity(sts_client, config, emit=emit)
    ensure_bucket(s3_client, config, emit=emit)
    apply_bucket_security(s3_client, config, emit=emit)
    apply_tls_bucket_policy(s3_client, config, emit=emit)
    ensure_iam_user(iam_client, config, emit=emit)
    put_user_policy(iam_client, config, emit=emit)
    maybe_create_access_key(iam_client, config, emit=emit)
