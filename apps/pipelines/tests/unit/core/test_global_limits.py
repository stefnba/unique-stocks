"""Tests for canonical Prefect global limit naming and settings."""

import pytest

from core.global_limits import (
    LAKE_WRITER_LIMIT,
    PREFECT_GLOBAL_LIMITS_STRICT_ENV_VAR,
    prefect_global_limits_strict,
    provider_rate_limit_name,
)


def test_provider_rate_limit_name_normalizes_provider_keys() -> None:
    """Provider limit names should be derived in one canonical place."""
    assert provider_rate_limit_name("EODHD_API") == "unique-stocks.http.provider.eodhd-api"


def test_lake_writer_limit_name_is_canonical() -> None:
    """The lake writer limit name should be shared by runtime and setup code."""
    assert LAKE_WRITER_LIMIT == "unique-stocks.lake-writer"


def test_prefect_global_limits_strict_defaults_to_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing strict flag should keep local/bootstrap flows fail-open."""
    monkeypatch.delenv(PREFECT_GLOBAL_LIMITS_STRICT_ENV_VAR, raising=False)

    assert prefect_global_limits_strict() is False


def test_prefect_global_limits_strict_rejects_invalid_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Invalid strict flag values should fail early instead of silently changing behavior."""
    monkeypatch.setenv(PREFECT_GLOBAL_LIMITS_STRICT_ENV_VAR, "maybe")

    with pytest.raises(ValueError, match="PREFECT_GLOBAL_LIMITS_STRICT"):
        prefect_global_limits_strict()
