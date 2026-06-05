import pytest

from scripts.setup_s3_landing_zone import (
    TLS_POLICY_SID,
    LandingZoneConfig,
    ProvisioningError,
    bucket_policy_statements,
    landing_policy,
    merge_tls_bucket_policy,
    tls_bucket_policy,
)


def _config(*, allow_delete: bool = False) -> LandingZoneConfig:
    """Build a minimal landing-zone config for pure policy tests."""
    return LandingZoneConfig(
        bucket_name="unique-stocks-dev",
        region="eu-central-1",
        iam_user="unique-stocks-dev-pipelines",
        policy_name="unique-stocks-dev-landing",
        profile=None,
        create_access_key=False,
        allow_delete=allow_delete,
        dry_run=True,
    )


def test_merge_tls_bucket_policy_builds_new_policy() -> None:
    """Missing bucket policy produces a TLS-only policy."""
    assert merge_tls_bucket_policy(None, "unique-stocks-dev") == tls_bucket_policy("unique-stocks-dev")


def test_merge_tls_bucket_policy_preserves_existing_statements() -> None:
    """Existing bucket policy statements are preserved when TLS enforcement is added."""
    existing_policy = {
        "Version": "2012-10-17",
        "Id": "existing-policy",
        "Statement": [
            {
                "Sid": "KeepMe",
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::123456789012:role/example"},
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::unique-stocks-dev/*",
            }
        ],
    }

    merged_policy = merge_tls_bucket_policy(existing_policy, "unique-stocks-dev")

    assert merged_policy["Id"] == "existing-policy"
    assert [statement["Sid"] for statement in merged_policy["Statement"]] == ["KeepMe", TLS_POLICY_SID]


def test_merge_tls_bucket_policy_replaces_existing_tls_statement() -> None:
    """Existing TLS statement is replaced so reruns stay idempotent."""
    existing_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Sid": TLS_POLICY_SID, "Effect": "Deny", "Action": "s3:OldAction"},
            {"Sid": "KeepMe", "Effect": "Deny", "Action": "s3:PutObject"},
        ],
    }

    merged_policy = merge_tls_bucket_policy(existing_policy, "unique-stocks-dev")
    statements = merged_policy["Statement"]

    assert [statement["Sid"] for statement in statements] == ["KeepMe", TLS_POLICY_SID]
    assert statements[-1]["Action"] == "s3:*"
    assert statements[-1]["Resource"] == [
        "arn:aws:s3:::unique-stocks-dev",
        "arn:aws:s3:::unique-stocks-dev/*",
    ]


def test_landing_policy_excludes_delete_by_default() -> None:
    """The IAM landing policy does not grant object deletion unless requested."""
    policy = landing_policy(_config())
    object_statement = policy["Statement"][1]

    assert object_statement["Sid"] == "ReadWriteLandingObjects"
    assert "s3:DeleteObject" not in object_statement["Action"]


def test_landing_policy_can_allow_delete_explicitly() -> None:
    """Object deletion is opt-in for cleanup workflows."""
    policy = landing_policy(_config(allow_delete=True))
    object_statement = policy["Statement"][1]

    assert "s3:DeleteObject" in object_statement["Action"]


def test_bucket_policy_statements_accepts_single_statement_mapping() -> None:
    """A policy with one Statement object is normalized to a list."""
    statements = bucket_policy_statements({"Statement": {"Sid": "OnlyStatement", "Effect": "Deny"}})

    assert statements == [{"Sid": "OnlyStatement", "Effect": "Deny"}]


def test_bucket_policy_statements_rejects_bad_shape() -> None:
    """Unexpected bucket policy shapes raise a provisioning error."""
    with pytest.raises(ProvisioningError, match="Statement must be an object or list"):
        bucket_policy_statements({"Statement": "not-a-statement"})
