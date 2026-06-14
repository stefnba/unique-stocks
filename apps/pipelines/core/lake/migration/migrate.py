"""Apply pending lake migrations."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

import duckdb

from core.lake import DataLakeClient
from core.lake.migration.files import MigrationFile
from core.lake.migration.refs import load_table_specs
from core.lake.migration.runner import apply_pending_migrations, plan_migrations
from core.lake.migration.validation import has_any_desired_table, validate_lake_schema
from core.lake.schema.table import TableModel
from core.prefect.limits import lake_writer_limit


def main(argv: Sequence[str] | None = None) -> None:
    """Apply pending migration SQL files to the configured lake.

    Args:
        argv: Optional CLI argument sequence for tests; ``None`` uses ``sys.argv``.
    """
    args = _parse_args(argv)
    migrations_dir = Path(args.migrations_dir)
    table_specs = load_table_specs(args.tables)
    client = DataLakeClient()
    try:
        if args.dry_run:
            plan = plan_migrations(client.connection, migrations_dir=migrations_dir)
            _print_plan(pending=plan.pending, skipped=plan.skipped)
            return
        if _should_validate_existing_schema_before_apply(client.connection, table_specs):
            validate_lake_schema(client.connection, tables=table_specs, allow_pending_changes=True)
        with lake_writer_limit("lake.migrate"):
            result = apply_pending_migrations(client.connection, migrations_dir=migrations_dir)
        validate_lake_schema(client.connection, tables=table_specs)
    finally:
        client.close()

    if not result.applied:
        print(f"No pending lake migrations. Skipped {len(result.skipped)} already-applied migration(s).")
        return
    print(
        f"Applied {len(result.applied)} lake migration(s); skipped {len(result.skipped)} already-applied migration(s)."
    )


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse CLI arguments for migration application.

    Args:
        argv: Optional CLI argument sequence for tests.

    Returns:
        Parsed argparse namespace.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--migrations-dir", required=True, help="Directory where migration SQL files are stored.")
    parser.add_argument(
        "--tables",
        required=True,
        help="Desired table specs as module:attribute.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print pending/applied migrations without applying.")
    return parser.parse_args(argv)


def _should_validate_existing_schema_before_apply(
    connection: duckdb.DuckDBPyConnection,
    table_specs: Sequence[type[TableModel]],
) -> bool:
    """Return whether an untracked lake already has app tables that need validation."""
    row = connection.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'lake' AND table_name = 'schema_migration'"
    ).fetchone()
    if row is not None:
        return False
    return has_any_desired_table(connection, tables=table_specs)


def _print_plan(*, pending: Sequence[MigrationFile], skipped: Sequence[MigrationFile]) -> None:
    """Print the dry-run migration plan grouped by pending and applied files.

    Args:
        pending: Migration files that would be applied.
        skipped: Migration files that are already recorded.
    """
    _print_migration_group("Pending lake migration(s)", pending)
    _print_migration_group("Already applied lake migration(s)", skipped)


def _print_migration_group(label: str, migrations: Sequence[MigrationFile]) -> None:
    """Print one dry-run migration group with version, name, and filename.

    Args:
        label: Human-readable group label.
        migrations: Migration files to print.
    """
    print(f"{label}: {len(migrations)}")
    if not migrations:
        print("  (none)")
        return
    for migration in migrations:
        print(f"  {migration.version} {migration.name} ({migration.filename})")


if __name__ == "__main__":
    main()
