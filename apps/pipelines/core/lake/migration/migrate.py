"""Apply pending lake migrations."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from core.clients.lake import DataLakeClient
from core.lake.migration.files import MigrationFile
from core.lake.migration.runner import apply_pending_migrations, plan_migrations


def main(argv: Sequence[str] | None = None) -> None:
    """Apply pending migration SQL files to the configured lake.

    Args:
        argv: Optional CLI argument sequence for tests; ``None`` uses ``sys.argv``.
    """
    args = _parse_args(argv)
    migrations_dir = Path(args.migrations_dir)
    client = DataLakeClient()
    try:
        if args.dry_run:
            plan = plan_migrations(client.connection, migrations_dir=migrations_dir)
            _print_plan(pending=plan.pending, skipped=plan.skipped)
            return
        result = apply_pending_migrations(client.connection, migrations_dir=migrations_dir)
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
    parser.add_argument("--dry-run", action="store_true", help="Print pending/applied migrations without applying.")
    return parser.parse_args(argv)


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
