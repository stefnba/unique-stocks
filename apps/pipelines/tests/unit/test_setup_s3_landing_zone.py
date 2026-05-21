from scripts.setup_s3_landing_zone import (
    TLS_POLICY_SID,
    merge_tls_bucket_policy,
    tls_bucket_policy,
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
