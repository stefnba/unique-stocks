"""Actual-vs-desired lake schema diffs."""

from dataclasses import dataclass

from core.lake.migration.introspection import ActualLakeSchema, ActualTable, DesiredLakeSchema, DesiredTable
from core.lake.schema.columns import ColumnSpec


@dataclass(frozen=True, slots=True)
class LakeSchemaDiff:
    """Rendered schema migration diff."""

    statements: tuple[str, ...]
    warnings: tuple[str, ...]
    suggested_name: str = "schema_diff"

    @property
    def has_changes(self) -> bool:
        """Return True when the diff has executable SQL or warnings."""
        return bool(self.statements or self.warnings)

    @property
    def has_statements(self) -> bool:
        """Return True when the diff has executable SQL statements."""
        return bool(self.statements)

    def to_sql(self) -> str:
        """Render the migration SQL file body."""
        sections: list[str] = [
            "-- Generated lake schema migration.",
            "-- Review before applying to shared environments.",
        ]
        if self.warnings:
            sections.append("--")
            sections.extend(f"-- WARNING: {warning}" for warning in self.warnings)
        if self.statements:
            sections.append("")
            sections.extend(statement.rstrip(";") + ";" for statement in self.statements)
        return "\n".join(sections).rstrip() + "\n"


def diff_lake_schema(actual: ActualLakeSchema, desired: DesiredLakeSchema) -> LakeSchemaDiff:
    """Generate a conservative migration diff from actual to desired state."""
    statements: list[str] = []
    warnings: list[str] = []
    missing_tables: list[DesiredTable] = []
    altered_tables: set[str] = set()

    for schema in sorted(desired.schemas - actual.schemas):
        statements.append(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    for key, desired_table in sorted(desired.tables.items()):
        actual_table = actual.tables.get(key)
        if actual_table is None:
            missing_tables.append(desired_table)
            statements.append(desired_table.ddl)
            continue
        table_statements, table_warnings = _diff_existing_table(actual_table, desired_table)
        if table_statements or table_warnings:
            altered_tables.add(desired_table.qualified_name)
        statements.extend(table_statements)
        warnings.extend(table_warnings)

    for key, actual_table in sorted(actual.tables.items()):
        if key not in desired.tables:
            warnings.append(f"Existing table {actual_table.qualified_name} is not in desired schema; review manually.")

    return LakeSchemaDiff(
        statements=tuple(statements),
        warnings=tuple(warnings),
        suggested_name=_suggest_name(
            missing_tables=tuple(missing_tables),
            altered_tables=tuple(sorted(altered_tables)),
            statements=tuple(statements),
            warnings=tuple(warnings),
        ),
    )


def _diff_existing_table(actual: ActualTable, desired: DesiredTable) -> tuple[list[str], list[str]]:
    statements: list[str] = []
    warnings: list[str] = []
    actual_columns = actual.columns
    desired_columns = {column.name: column for column in desired.columns}

    for column in desired.columns:
        actual_column = actual_columns.get(column.name)
        if actual_column is None:
            column_statements, column_warning = _add_column_statements(desired, column)
            statements.extend(column_statements)
            if column_warning is not None:
                warnings.append(column_warning)
            continue
        if not _sql_types_match(column.sql_type, actual_column.sql_type):
            warnings.append(
                f"{desired.qualified_name}.{column.name} type differs "
                f"(actual {actual_column.sql_type}, desired {column.sql_type}); review manually."
            )
        if column.nullable != actual_column.nullable:
            warnings.append(
                f"{desired.qualified_name}.{column.name} nullability differs "
                f"(actual {'nullable' if actual_column.nullable else 'not null'}, "
                f"desired {'nullable' if column.nullable else 'not null'}); review manually."
            )
        if column.default is not None and not _defaults_match(column.default, actual_column.default):
            warnings.append(
                f"{desired.qualified_name}.{column.name} default differs "
                f"(actual {actual_column.default or 'none'}, desired {column.default}); review manually."
            )

    for column_name in sorted(set(actual_columns) - set(desired_columns)):
        warnings.append(
            f"Existing column {desired.qualified_name}.{column_name} is not in desired schema; review manually."
        )

    desired_unique = tuple(desired.unique_columns)
    if desired_unique and desired_unique not in actual.unique_constraints:
        warnings.append(
            f"{desired.qualified_name} unique constraint {desired_unique} is missing or changed; review manually."
        )

    return statements, warnings


def _add_column_statements(table: DesiredTable, column: ColumnSpec) -> tuple[list[str], str | None]:
    if column.nullable:
        return [f"ALTER TABLE {table.qualified_name} ADD COLUMN IF NOT EXISTS {column.to_ddl()}"], None
    if column.default is not None:
        nullable_column = ColumnSpec(column.name, column.sql_type, nullable=True, default=column.default)
        return [
            f"ALTER TABLE {table.qualified_name} ADD COLUMN IF NOT EXISTS {nullable_column.to_ddl()}",
            f"ALTER TABLE {table.qualified_name} ALTER COLUMN {column.name} SET NOT NULL",
        ], None
    return [], f"Missing non-null column {table.qualified_name}.{column.name} has no default; write manually."


def _sql_types_match(desired_type: str, actual_type: str) -> bool:
    desired = _normalize_sql_type(desired_type)
    actual = _normalize_sql_type(actual_type)
    if desired == "DECIMAL" and actual.startswith("DECIMAL("):
        return True
    return desired == actual


def _normalize_sql_type(sql_type: str) -> str:
    normalized = " ".join(sql_type.upper().split())
    aliases = {
        "TEXT": "VARCHAR",
        "TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE",
    }
    return aliases.get(normalized, normalized)


def _defaults_match(desired_default: str, actual_default: str | None) -> bool:
    if actual_default is None:
        return False
    return _normalize_default(desired_default) == _normalize_default(actual_default)


def _normalize_default(default: str) -> str:
    return "".join(default.lower().split())


def _suggest_name(
    *,
    missing_tables: tuple[DesiredTable, ...],
    altered_tables: tuple[str, ...],
    statements: tuple[str, ...],
    warnings: tuple[str, ...],
) -> str:
    if len(missing_tables) == 1 and not altered_tables and not warnings:
        table = missing_tables[0]
        return f"create_{table.schema}_{table.name}"
    if len(altered_tables) == 1 and not missing_tables:
        return f"alter_{altered_tables[0].replace('.', '_')}"
    if statements and all(statement.startswith("CREATE SCHEMA") for statement in statements) and not warnings:
        return "create_schemas"
    return "schema_diff"
