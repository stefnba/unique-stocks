"""Tests for the S3 landing-zone CLI adapter."""

from scripts.s3 import setup_landing_zone


def test_parse_args_builds_landing_zone_config() -> None:
    """CLI args should resolve to the core provisioning config."""
    config = setup_landing_zone.parse_args(
        [
            "--bucket",
            "unique-stocks-prod",
            "--region",
            "eu-central-1",
            "--user",
            "unique-stocks-prod-pipelines",
            "--policy-name",
            "unique-stocks-prod-landing",
            "--profile",
            "provisioner",
            "--allow-delete",
            "--create-access-key",
            "--dry-run",
        ]
    )

    assert config.bucket_name == "unique-stocks-prod"
    assert config.region == "eu-central-1"
    assert config.iam_user == "unique-stocks-prod-pipelines"
    assert config.policy_name == "unique-stocks-prod-landing"
    assert config.profile == "provisioner"
    assert config.allow_delete is True
    assert config.create_access_key is True
    assert config.dry_run is True
