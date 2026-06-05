"""Ensure the configured lake database exists before connecting."""

from __future__ import annotations

from pathlib import Path

import duckdb
import structlog

from config.settings import Settings

log = structlog.get_logger(__name__)


def ensure_lake_database(settings: Settings) -> None:
    """Create the lake database or local file when it does not exist yet.

    MotherDuck requires an explicit ``CREATE DATABASE`` before ``md:<name>`` can
    connect. Local DuckDB creates the file on connect, so local bootstrap opens
    and closes the configured path without creating lake schemas or tables.

    Args:
        settings: Application settings with lake backend and path configuration.
    """
    backend = settings.lake_backend()
    if backend == "motherduck":
        _ensure_motherduck_database(
            database_name=settings.motherduck_database_name,
            motherduck_token=settings.motherduck_token.get_secret_value(),
        )
        return
    _ensure_local_lake_database(settings.resolved_local_lake_path())


def motherduck_connection_string(
    *,
    database_name: str,
    motherduck_token: str,
) -> str:
    """Build a MotherDuck DuckDB connection string for the project lake.

    Args:
        database_name: MotherDuck database name.
        motherduck_token: MotherDuck API token.

    Returns:
        Connection string suitable for ``duckdb.connect``.
    """
    return f"md:{database_name}?motherduck_token={motherduck_token}"


def _ensure_motherduck_database(*, database_name: str, motherduck_token: str) -> None:
    """Create the MotherDuck database if it is missing.

    Args:
        database_name: MotherDuck database to create.
        motherduck_token: MotherDuck API token.
    """
    token = motherduck_token.strip()
    if not token:
        return

    bootstrap_conn_str = f"md:?motherduck_token={token}"
    log.info("lake.ensure_database", backend="motherduck", database=database_name)
    conn = duckdb.connect(bootstrap_conn_str)
    try:
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(database_name)}")
    finally:
        conn.close()


def _ensure_local_lake_database(path: str) -> None:
    """Ensure a local DuckDB database file exists.

    Args:
        path: Absolute or relative path to the DuckDB file.
    """
    lake_path = Path(path)
    if lake_path.parent != lake_path:
        lake_path.parent.mkdir(parents=True, exist_ok=True)
    if not lake_path.exists():
        conn = duckdb.connect(str(lake_path))
        conn.close()
    log.info("lake.ensure_database", backend="local", path=str(lake_path))


def _quote_identifier(value: str) -> str:
    """Quote a DuckDB identifier for safe SQL interpolation.

    Args:
        value: Raw DuckDB identifier.

    Returns:
        Safely quoted identifier.

    Raises:
        ValueError: If the identifier is empty or contains a NUL byte.
    """
    if not value or "\x00" in value:
        raise ValueError(f"Invalid DuckDB identifier: {value!r}")
    return '"' + value.replace('"', '""') + '"'
