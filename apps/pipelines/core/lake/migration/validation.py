"""Post-migration lake schema validation."""

from __future__ import annotations

from collections.abc import Sequence

import duckdb

from core.lake.migration.diff import diff_lake_schema
from core.lake.migration.introspection import desired_lake_schema_from_tables, inspect_lake_schema
from core.lake.schema.ddl import DEFAULT_SCHEMAS
from core.lake.schema.table import TableModel


class LakeSchemaValidationError(RuntimeError):
    """Raised when the physical lake schema does not match table specs."""


def validate_lake_schema(
    connection: duckdb.DuckDBPyConnection,
    *,
    tables: Sequence[type[TableModel]],
    allow_pending_changes: bool = False,
) -> None:
    """Raise when the connected lake does not match the desired table specs."""
    issues = lake_schema_validation_issues(
        connection,
        tables=tables,
        allow_pending_changes=allow_pending_changes,
    )
    if issues:
        joined = "\n".join(f"- {issue}" for issue in issues)
        raise LakeSchemaValidationError(f"Lake schema validation failed:\n{joined}")


def lake_schema_validation_issues(
    connection: duckdb.DuckDBPyConnection,
    *,
    tables: Sequence[type[TableModel]],
    allow_pending_changes: bool = False,
) -> tuple[str, ...]:
    """Return schema validation issues, ignoring allowed runtime tables."""
    desired = desired_lake_schema_from_tables(tuple(tables), default_schemas=DEFAULT_SCHEMAS)
    actual = inspect_lake_schema(connection, schemas=desired.schemas)
    diff = diff_lake_schema(actual, desired)
    issues: list[str] = []
    if not allow_pending_changes:
        issues.extend(diff.statements)
    issues.extend(warning for warning in diff.warnings if not _is_ignored_extra_table_warning(warning))
    return tuple(issues)


def has_any_desired_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    tables: Sequence[type[TableModel]],
) -> bool:
    """Return whether any desired application table already exists."""
    desired = desired_lake_schema_from_tables(tuple(tables), default_schemas=DEFAULT_SCHEMAS)
    actual = inspect_lake_schema(connection, schemas=desired.schemas)
    return bool(set(actual.tables).intersection(desired.tables))


def _is_ignored_extra_table_warning(warning: str) -> bool:
    prefix = "Existing table "
    suffix = " is not in desired schema; review manually."
    if not warning.startswith(prefix) or not warning.endswith(suffix):
        return False
    qualified = warning[len(prefix) : -len(suffix)]
    schema, separator, table = qualified.partition(".")
    if separator != ".":
        return False
    return schema in {"silver", "gold"} or (schema == "lake" and table == "schema_migration")


__all__ = [
    "LakeSchemaValidationError",
    "has_any_desired_table",
    "lake_schema_validation_issues",
    "validate_lake_schema",
]
