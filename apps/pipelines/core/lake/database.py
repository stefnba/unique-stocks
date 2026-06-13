"""Ensure a lake database exists before connecting."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import duckdb
import structlog

log = structlog.get_logger(__name__)


class SecretValue(Protocol):
    """Small protocol for secret wrappers such as Pydantic ``SecretStr``."""

    def get_secret_value(self) -> str:
        """Return the raw secret value."""
        ...


class LakeDatabaseSettings(Protocol):
    """Settings shape needed to bootstrap a lake database."""

    @property
    def motherduck_database_name(self) -> str:
        """Return the MotherDuck database name."""
        ...

    @property
    def motherduck_token(self) -> SecretValue:
        """Return the wrapped MotherDuck token."""
        ...

    def lake_backend(self) -> str:
        """Return the active lake backend."""
        ...

    def resolved_local_lake_path(self) -> str:
        """Return the absolute local DuckDB path."""
        ...


def ensure_lake_database(settings: LakeDatabaseSettings) -> None:
    """Create the lake database or local file when it does not exist yet.

    MotherDuck requires an explicit ``CREATE DATABASE`` before ``md:<name>`` can
    connect. Local DuckDB creates the file on connect, so local bootstrap opens
    and closes the configured path without creating lake schemas or tables.

    Args:
        settings: Application settings with lake backend and path configuration.
    """
    ensure_lake_database_for_backend(
        backend=settings.lake_backend(),
        database_name=settings.motherduck_database_name,
        motherduck_token=settings.motherduck_token.get_secret_value(),
        local_lake_path=settings.resolved_local_lake_path(),
    )


def ensure_lake_database_for_backend(
    *,
    backend: str,
    database_name: str,
    motherduck_token: str,
    local_lake_path: str,
) -> None:
    """Create the concrete lake database selected by primitive runtime values."""
    if backend == "motherduck":
        _ensure_motherduck_database(
            database_name=database_name,
            motherduck_token=motherduck_token,
        )
        return
    _ensure_local_lake_database(local_lake_path)


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
