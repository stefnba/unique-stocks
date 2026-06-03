"""DDL rendering helpers for table models."""

from collections.abc import Sequence

from core.lake.schema.table import TableModel

DEFAULT_SCHEMAS = ("bronze", "silver", "gold", "pipeline", "lake")


def render_greenfield_schema_sql(tables: Sequence[type[TableModel]]) -> str:
    """Render the complete idempotent greenfield lake schema SQL."""
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
    return "\n".join(
        (
            "-- Initialise unique_stocks schemas and tables.",
            "-- Generated from Python table specs.",
            "-- Use lake migrations to apply schema changes.",
        )
    )


def _schema_statements() -> tuple[str, ...]:
    return tuple(f"CREATE SCHEMA IF NOT EXISTS {schema_name};" for schema_name in DEFAULT_SCHEMAS)


def _section(title: str) -> str:
    rule = "-- -----------------------------------------------------------------------"
    return "\n".join((rule, f"-- {title}", rule))


def _table_statements(tables: Sequence[type[TableModel]], *, schema_name: str) -> tuple[str, ...]:
    return tuple(table.to_ddl() for table in tables if table.schema_name == schema_name)
