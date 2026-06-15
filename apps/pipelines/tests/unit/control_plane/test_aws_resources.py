"""Tests for app AWS resource-name wiring."""

import pytest

from config.settings import get_settings
from control_plane.aws_resources import aws_resource_defaults


def test_aws_resource_defaults_uses_dev_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """AWS resource defaults should use dev names outside production."""
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    get_settings.cache_clear()
    try:
        defaults = aws_resource_defaults()
    finally:
        get_settings.cache_clear()

    assert defaults.bucket_name == "unique-stocks-dev"
    assert defaults.region == "eu-central-1"
    assert defaults.iam_user == "unique-stocks-pipelines-dev"
    assert defaults.inline_policy_name == "unique-stocks-pipelines-s3-landing-dev"


def test_aws_resource_defaults_uses_prod_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """AWS resource defaults should use prod names in production."""
    monkeypatch.setenv("ENVIRONMENT", "prod")
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "test-token")
    get_settings.cache_clear()
    try:
        defaults = aws_resource_defaults()
    finally:
        get_settings.cache_clear()

    assert defaults.bucket_name == "unique-stocks-prod"
    assert defaults.region == "eu-central-1"
    assert defaults.iam_user == "unique-stocks-pipelines-prod"
    assert defaults.inline_policy_name == "unique-stocks-pipelines-s3-landing-prod"
