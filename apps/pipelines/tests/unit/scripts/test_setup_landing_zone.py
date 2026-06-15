"""Tests for the S3 landing-zone CLI adapter."""

import pytest

from config.settings import get_settings
from scripts.s3 import setup_landing_zone


def test_parse_args_uses_dev_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """CLI defaults should come from the app AWS resource registry."""
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    get_settings.cache_clear()

    try:
        config = setup_landing_zone.parse_args(["--dry-run"])
    finally:
        get_settings.cache_clear()

    assert config.bucket_name == "unique-stocks-dev"
    assert config.region == "eu-central-1"
    assert config.iam_user == "unique-stocks-pipelines-dev"
    assert config.policy_name == "unique-stocks-pipelines-s3-landing-dev"


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
