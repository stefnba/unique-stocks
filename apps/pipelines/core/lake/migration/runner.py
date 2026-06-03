"""Migration application for lake schema SQL files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import duckdb
import structlog

from core.lake.migration.files import MigrationFile, list_migration_files

log = structlog.get_logger(__name__)


class MigrationChecksumError(RuntimeError):
    """Raised when an applied migration file has changed on disk."""


@dataclass(frozen=True, slots=True)
class AppliedMigration:
    """One migration recorded in the lake."""

    version: str
    name: str
    checksum: str


@dataclass(frozen=True, slots=True)
class MigrationRunResult:
    """Result of applying pending migrations."""

    applied: tuple[MigrationFile, ...]
    skipped: tuple[MigrationFile, ...]


@dataclass(frozen=True, slots=True)
class MigrationPlan:
    """Pending and already-applied migration files."""

    pending: tuple[MigrationFile, ...]
    skipped: tuple[MigrationFile, ...]


def plan_migrations(
    connection: duckdb.DuckDBPyConnection,
    *,
    migrations_dir: Path,
    tracking_schema: str = "lake",
    tracking_table: str = "schema_migration",
) -> MigrationPlan:
    """Plan migrations without mutating the lake."""
    if _table_exists(connection, schema=tracking_schema, table=tracking_table):
        applied_by_version = load_applied_migrations(connection, schema=tracking_schema, table=tracking_table)
    else:
        applied_by_version = {}

    pending: list[MigrationFile] = []
    skipped: list[MigrationFile] = []
    for migration in list_migration_files(migrations_dir):
        recorded = applied_by_version.get(migration.version)
        if recorded is None:
            pending.append(migration)
            continue
        if recorded.checksum != migration.checksum:
            raise MigrationChecksumError(
                f"Applied migration {migration.filename} checksum changed "
                f"(lake {recorded.checksum}, file {migration.checksum})"
            )
        skipped.append(migration)
    return MigrationPlan(pending=tuple(pending), skipped=tuple(skipped))


def apply_pending_migrations(
    connection: duckdb.DuckDBPyConnection,
    *,
    migrations_dir: Path,
    tracking_schema: str = "lake",
    tracking_table: str = "schema_migration",
) -> MigrationRunResult:
    """Apply pending migration files in order."""
    ensure_migration_table(connection, schema=tracking_schema, table=tracking_table)
    plan = plan_migrations(
        connection,
        migrations_dir=migrations_dir,
        tracking_schema=tracking_schema,
        tracking_table=tracking_table,
    )

    applied: list[MigrationFile] = []
    for migration in plan.pending:
        _apply_one_migration(
            connection,
            migration,
            tracking_schema=tracking_schema,
            tracking_table=tracking_table,
        )
        applied.append(migration)

    return MigrationRunResult(applied=tuple(applied), skipped=plan.skipped)


def ensure_migration_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    schema: str = "lake",
    table: str = "schema_migration",
) -> None:
    """Create the migration tracking table if needed."""
    qualified = _qualified(schema, table)
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified} (
            version VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            checksum VARCHAR NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            execution_seconds DOUBLE NOT NULL,
            UNIQUE (version)
        )
        """
    )


def load_applied_migrations(
    connection: duckdb.DuckDBPyConnection,
    *,
    schema: str = "lake",
    table: str = "schema_migration",
) -> dict[str, AppliedMigration]:
    """Load applied migrations keyed by version."""
    rows = connection.execute(
        f"""
        SELECT version, name, checksum
        FROM {_qualified(schema, table)}
        ORDER BY version
        """
    ).fetchall()
    return {
        str(version): AppliedMigration(version=str(version), name=str(name), checksum=str(checksum))
        for version, name, checksum in rows
    }


def _apply_one_migration(
    connection: duckdb.DuckDBPyConnection,
    migration: MigrationFile,
    *,
    tracking_schema: str,
    tracking_table: str,
) -> None:
    sql = migration.path.read_text(encoding="utf-8")
    started = perf_counter()
    try:
        connection.execute("BEGIN")
        connection.execute(sql)
        execution_seconds = perf_counter() - started
        connection.execute(
            f"""
            INSERT INTO {_qualified(tracking_schema, tracking_table)}
                (version, name, checksum, execution_seconds)
            VALUES (?, ?, ?, ?)
            """,
            [migration.version, migration.name, migration.checksum, execution_seconds],
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        log.exception("lake.migration_failed", version=migration.version, name=migration.name)
        raise

    log.info(
        "lake.migration_applied",
        version=migration.version,
        name=migration.name,
        execution_seconds=execution_seconds,
    )


def _qualified(schema: str, table: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def _table_exists(connection: duckdb.DuckDBPyConnection, *, schema: str, table: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
        is not None
    )


def _quote_identifier(value: str) -> str:
    if not value or "\x00" in value:
        raise ValueError(f"Invalid DuckDB identifier: {value!r}")
    return '"' + value.replace('"', '""') + '"'
