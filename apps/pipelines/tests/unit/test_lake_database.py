"""Tests for lake database bootstrap helpers."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from pydantic import SecretStr

from config.settings import APP_ROOT, Settings
from core.lake.database import ensure_lake_database, motherduck_connection_string


def test_default_local_lake_path_matches_lake_name() -> None:
    """Local file default should stay aligned with the logical lake name."""
    settings = Settings()
    assert settings.lake_name == "unique_stocks"
    assert Path(settings.resolved_local_lake_path()) == APP_ROOT / "unique_stocks.duckdb"


def test_motherduck_connection_string_uses_database_name() -> None:
    """MotherDuck connections target the configured database."""
    assert (
        motherduck_connection_string(database_name="unique_stocks", motherduck_token="secret")
        == "md:unique_stocks?motherduck_token=secret"
    )
    assert (
        motherduck_connection_string(database_name="sandbox", motherduck_token="secret")
        == "md:sandbox?motherduck_token=secret"
    )


def test_ensure_motherduck_uses_settings_database_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bootstrap should use LAKE_NAME from settings."""
    from unittest.mock import MagicMock, patch

    conn = MagicMock()
    with patch("core.lake.database.duckdb.connect", return_value=conn) as connect:
        settings = Settings(motherduck_token=SecretStr("tok"), lake_name="sandbox")
        ensure_lake_database(settings)

    conn.execute.assert_called_once_with('CREATE DATABASE IF NOT EXISTS "sandbox"')
    connect.assert_called_once_with("md:?motherduck_token=tok")


def test_ensure_local_lake_path_creates_parent_directory(tmp_path: Path) -> None:
    """Local bootstrap should create missing parent directories and the database file."""
    lake_file = tmp_path / "nested" / "lake.duckdb"
    settings = Settings(motherduck_token=SecretStr(""), local_lake_path=str(lake_file))

    ensure_lake_database(settings)

    assert lake_file.parent.is_dir()
    assert lake_file.is_file()


def test_ensure_local_lake_database_leaves_schema_empty(tmp_path: Path) -> None:
    """A freshly bootstrapped local lake should diff like an empty catalog."""
    lake_file = tmp_path / "lake.duckdb"
    settings = Settings(motherduck_token=SecretStr(""), local_lake_path=str(lake_file))

    ensure_lake_database(settings)

    conn = duckdb.connect(str(lake_file), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('bronze', 'silver', 'gold', 'pipeline', 'lake')
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == []


@patch("core.lake.database.duckdb.connect")
def test_ensure_motherduck_database_creates_when_missing(connect: MagicMock) -> None:
    """MotherDuck bootstrap should create the project database before use."""
    conn = MagicMock()
    connect.return_value = conn
    settings = Settings(motherduck_token=SecretStr("test-token"))

    ensure_lake_database(settings)

    connect.assert_called_once_with("md:?motherduck_token=test-token")
    conn.execute.assert_called_once_with('CREATE DATABASE IF NOT EXISTS "unique_stocks"')
    conn.close.assert_called_once()


@patch("core.lake.database.duckdb.connect")
def test_ensure_motherduck_blank_token_uses_local_bootstrap(connect: MagicMock, tmp_path: Path) -> None:
    """Whitespace MotherDuck tokens should fall back to local bootstrap."""
    lake_file = tmp_path / "lake.duckdb"
    settings = Settings(motherduck_token=SecretStr("   "), local_lake_path=str(lake_file))

    ensure_lake_database(settings)

    connect.assert_called_once_with(str(lake_file))
    connect.return_value.close.assert_called_once()
