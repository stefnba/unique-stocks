"""Tests for central pipeline logging configuration."""

import json

import structlog
from pydantic import SecretStr
from pytest import CaptureFixture

from config.settings import Settings
from core.utils.logging import REDACTED_LOG_VALUE, configure_logging


def test_configure_logging_renders_json_and_redacts_sensitive_values(capsys: CaptureFixture[str]) -> None:
    """Production logging should emit JSON and redact known secret shapes."""
    settings = Settings(
        environment="prod",
        motherduck_token=SecretStr("motherduck-token"),
        pipeline_log_format="json",
    )
    configure_logging(settings=settings, force=True)

    structlog.get_logger("tests.logging").info(
        "logging.redaction_check",
        api_token="provider-token",
        url="https://provider.example/prices?api_token=provider-token&symbol=AAPL.US",
        nested={"password": "secret-password", "safe": "ok"},
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["event"] == "logging.redaction_check"
    assert payload["service"] == "pipelines"
    assert payload["environment"] == "prod"
    assert payload["api_token"] == REDACTED_LOG_VALUE
    assert payload["nested"]["password"] == REDACTED_LOG_VALUE
    assert payload["nested"]["safe"] == "ok"
    assert "provider-token" not in json.dumps(payload)
    assert f"api_token={REDACTED_LOG_VALUE}" in payload["url"]


def test_configure_logging_is_idempotent_without_force(capsys: CaptureFixture[str]) -> None:
    """A later non-forced call should not rebuild an existing process config."""
    prod_settings = Settings(
        environment="prod",
        motherduck_token=SecretStr("motherduck-token"),
        pipeline_log_format="json",
    )
    dev_settings = Settings(environment="dev", pipeline_log_format="console")
    configure_logging(settings=prod_settings, force=True)
    capsys.readouterr()

    configure_logging(settings=dev_settings)
    structlog.get_logger("tests.logging").info("logging.idempotent_check")

    payload = json.loads(capsys.readouterr().out)
    assert payload["event"] == "logging.idempotent_check"
    assert payload["environment"] == "prod"
