"""Tests for application settings path resolution."""

from pathlib import Path

import pytest
from pydantic import SecretStr

from config.settings import APP_ROOT, PROD_MOTHERDUCK_ERROR, Settings, get_settings


def test_resolved_local_lake_path_is_absolute() -> None:
    """Relative lake paths should resolve against the pipelines app root."""
    settings = Settings(local_lake_path="unique_stocks.duckdb")
    assert Path(settings.resolved_local_lake_path()) == APP_ROOT / "unique_stocks.duckdb"


def test_resolved_local_lake_path_derives_from_lake_name_when_unset() -> None:
    """Unset local lake paths should derive from the logical lake name."""
    settings = Settings(lake_name="sandbox_lake", local_lake_path="")
    assert Path(settings.resolved_local_lake_path()) == APP_ROOT / "sandbox_lake.duckdb"


def test_resolved_local_lake_path_keeps_absolute_input() -> None:
    """Absolute lake paths should pass through unchanged."""
    absolute = "/tmp/test_lake.duckdb"
    settings = Settings(local_lake_path=absolute)
    assert settings.resolved_local_lake_path() == absolute


def test_lake_backend_uses_local_without_motherduck_token() -> None:
    """Blank MotherDuck tokens should select the local lake backend."""
    settings = Settings(motherduck_token=SecretStr(""))
    assert settings.lake_backend() == "local"


def test_lake_backend_treats_whitespace_motherduck_token_as_local() -> None:
    """Whitespace-only MotherDuck tokens should not select MotherDuck."""
    settings = Settings(motherduck_token=SecretStr("   "))
    assert settings.lake_backend() == "local"
    assert settings.resolved_dbt_target() == "dev"


def test_lake_backend_uses_motherduck_with_token() -> None:
    """A configured MotherDuck token should select the MotherDuck backend."""
    settings = Settings(motherduck_token=SecretStr("test-token"))
    assert settings.lake_backend() == "motherduck"


def test_prefect_lake_writer_limit_defaults_to_local_serial_writes() -> None:
    """Local DuckDB should default to one writer slot."""
    settings = Settings(motherduck_token=SecretStr(""))

    assert settings.default_prefect_lake_writer_limit() == 1
    assert settings.resolved_prefect_lake_writer_limit() == 1


def test_prefect_lake_writer_limit_defaults_higher_for_motherduck() -> None:
    """MotherDuck can use a modestly higher default writer limit."""
    settings = Settings(motherduck_token=SecretStr("test-token"))

    assert settings.default_prefect_lake_writer_limit() == 4
    assert settings.resolved_prefect_lake_writer_limit() == 4


def test_prefect_lake_writer_limit_can_be_overridden() -> None:
    """Operators can override the backend-aware default explicitly."""
    settings = Settings(motherduck_token=SecretStr("test-token"), prefect_lake_writer_limit=2)

    assert settings.resolved_prefect_lake_writer_limit() == 2


def test_empty_prefect_lake_writer_limit_env_is_ignored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Blank env overrides should fall back to backend-aware defaults."""
    monkeypatch.setenv("PREFECT_LAKE_WRITER_LIMIT", "")
    get_settings.cache_clear()
    try:
        settings = Settings(motherduck_token=SecretStr(""))
    finally:
        get_settings.cache_clear()

    assert settings.prefect_lake_writer_limit is None
    assert settings.resolved_prefect_lake_writer_limit() == 1


def test_resolved_dbt_target_matches_lake_backend() -> None:
    """Dbt target should follow the selected lake backend."""
    assert Settings(motherduck_token=SecretStr("")).resolved_dbt_target() == "dev"
    assert Settings(motherduck_token=SecretStr("test-token")).resolved_dbt_target() == "prod"


def test_dbt_env_overlay_uses_absolute_local_lake_path() -> None:
    """Local dbt env should target the same absolute DuckDB file as Python ingestion."""
    local_path = str(APP_ROOT / "unique_stocks.duckdb")
    settings = Settings(motherduck_token=SecretStr(""), local_lake_path="unique_stocks.duckdb")
    assert settings.dbt_env_overlay() == {
        "DBT_TARGET": "dev",
        "LAKE_NAME": "unique_stocks",
        "LOCAL_LAKE_PATH": local_path,
        "DBT_DUCKDB_PATH": local_path,
    }


def test_dbt_env_overlay_uses_prod_for_motherduck() -> None:
    """MotherDuck dbt env should pass the token to dbt without setting DBT_DUCKDB_PATH."""
    settings = Settings(motherduck_token=SecretStr("test-token"), local_lake_path="unique_stocks.duckdb")
    assert settings.dbt_env_overlay() == {
        "DBT_TARGET": "prod",
        "LAKE_NAME": "unique_stocks",
        "LOCAL_LAKE_PATH": str(APP_ROOT / "unique_stocks.duckdb"),
        "MOTHERDUCK_TOKEN": "test-token",
    }


def test_lake_name_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """LAKE_NAME env should override the code fallback."""
    monkeypatch.setenv("LAKE_NAME", "sandbox_lake")
    get_settings.cache_clear()
    try:
        assert Settings().lake_name == "sandbox_lake"
    finally:
        get_settings.cache_clear()


def test_prod_requires_motherduck_token() -> None:
    """Production settings should fail closed instead of falling back to local DuckDB."""
    with pytest.raises(ValueError, match=PROD_MOTHERDUCK_ERROR):
        Settings(environment="prod", motherduck_token=SecretStr(""))


def test_prod_with_motherduck_token_is_valid() -> None:
    """Production settings should allow the MotherDuck backend."""
    settings = Settings(environment="prod", motherduck_token=SecretStr("test-token"))
    assert settings.lake_backend() == "motherduck"
    assert settings.resolved_dbt_target() == "prod"
