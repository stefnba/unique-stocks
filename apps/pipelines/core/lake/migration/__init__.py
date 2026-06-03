"""Lake schema migration helpers."""

from core.lake.migration.diff import LakeSchemaDiff, diff_lake_schema
from core.lake.migration.files import MigrationFile, write_migration_file
from core.lake.migration.introspection import (
    ActualLakeSchema,
    DesiredLakeSchema,
    desired_lake_schema_from_tables,
    inspect_lake_schema,
)
from core.lake.migration.runner import MigrationPlan, MigrationRunResult, apply_pending_migrations, plan_migrations

__all__ = [
    "ActualLakeSchema",
    "DesiredLakeSchema",
    "LakeSchemaDiff",
    "MigrationFile",
    "MigrationPlan",
    "MigrationRunResult",
    "apply_pending_migrations",
    "desired_lake_schema_from_tables",
    "diff_lake_schema",
    "inspect_lake_schema",
    "plan_migrations",
    "write_migration_file",
]
