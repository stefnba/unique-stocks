"""DDL rendering helpers for table models."""

from collections.abc import Sequence

from core.lake.schema.table import TableModel

DEFAULT_SCHEMAS = ("bronze", "silver", "gold", "pipeline", "lake")


def render_greenfield_schema_sql(tables: Sequence[type[TableModel]]) -> str:
    """Render the complete idempotent greenfield lake schema SQL.

    Args:
        tables: Table model classes to include in the greenfield SQL.

    Returns:
        SQL text for creating all lake schemas and registered tables.
    """
    sections = [
        _header(),
        *_schema_statements(),
        "",
        _section("Bronze - raw, immutable, append-only"),
        *_table_statements(tables, schema_name="bronze"),
        "",
        _section("Pipeline run tracking"),
        *_table_statements(tables, schema_name="pipeline"),
    ]
    return "\n\n".join(section for section in sections if section).rstrip() + "\n"


def _header() -> str:
    """Return the standard header for rendered greenfield SQL.

    Returns:
        SQL comment header text.
    """
    return "\n".join(
        (
            "-- Initialise unique_stocks schemas and tables.",
            "-- Generated from Python table specs.",
            "-- Use lake migrations to apply schema changes.",
        )
    )


def _schema_statements() -> tuple[str, ...]:
    """Return idempotent schema creation statements for all lake schemas.

    Returns:
        SQL statements that create the default schemas if missing.
    """
    return tuple(f"CREATE SCHEMA IF NOT EXISTS {schema_name};" for schema_name in DEFAULT_SCHEMAS)


def _section(title: str) -> str:
    """Render a comment divider for a greenfield SQL section.

    Args:
        title: Section title to include in the divider.

    Returns:
        SQL comment divider text.
    """
    rule = "-- -----------------------------------------------------------------------"
    return "\n".join((rule, f"-- {title}", rule))


def _table_statements(tables: Sequence[type[TableModel]], *, schema_name: str) -> tuple[str, ...]:
    """Return DDL statements for tables that belong to one schema.

    Args:
        tables: Table model classes to filter.
        schema_name: Lake schema name to render.

    Returns:
        DDL statements for matching tables.
    """
    return tuple(table.to_ddl() for table in tables if table.schema_name == schema_name)
