"""Tests for application settings path resolution."""

from pathlib import Path

import pytest
from pydantic import SecretStr

from config.settings import APP_ROOT, PROD_MOTHERDUCK_ERROR, Settings


def test_resolved_local_lake_path_is_absolute() -> None:
    """Relative lake paths should resolve against the pipelines app root."""
    settings = Settings(local_lake_path="unique_stocks.duckdb")
    assert Path(settings.resolved_local_lake_path()) == APP_ROOT / "unique_stocks.duckdb"


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


def test_resolved_dbt_target_matches_lake_backend() -> None:
    """Dbt target should follow the selected lake backend."""
    assert Settings(motherduck_token=SecretStr("")).resolved_dbt_target() == "dev"
    assert Settings(motherduck_token=SecretStr("test-token")).resolved_dbt_target() == "prod"


def test_dbt_env_overlay_uses_absolute_local_lake_path() -> None:
    """Local dbt env should target the same absolute DuckDB file as Python ingestion."""
    settings = Settings(motherduck_token=SecretStr(""), local_lake_path="unique_stocks.duckdb")
    assert settings.dbt_env_overlay() == {
        "DBT_TARGET": "dev",
        "DBT_DUCKDB_PATH": str(APP_ROOT / "unique_stocks.duckdb"),
    }


def test_dbt_env_overlay_uses_prod_for_motherduck() -> None:
    """MotherDuck dbt env should switch target without setting a local DuckDB path."""
    settings = Settings(motherduck_token=SecretStr("test-token"))
    assert settings.dbt_env_overlay() == {"DBT_TARGET": "prod"}


def test_prod_requires_motherduck_token() -> None:
    """Production settings should fail closed instead of falling back to local DuckDB."""
    with pytest.raises(ValueError, match=PROD_MOTHERDUCK_ERROR):
        Settings(environment="prod", motherduck_token=SecretStr(""))


def test_prod_with_motherduck_token_is_valid() -> None:
    """Production settings should allow the MotherDuck backend."""
    settings = Settings(environment="prod", motherduck_token=SecretStr("test-token"))
    assert settings.lake_backend() == "motherduck"
    assert settings.resolved_dbt_target() == "prod"
