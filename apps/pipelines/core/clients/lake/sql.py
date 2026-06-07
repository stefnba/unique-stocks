"""SQL file rendering helpers for lake reads and writes."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any

from jinja2 import Environment, StrictUndefined

type SqlTemplateContext = Mapping[str, Any]


def render_sql_file(
    sql_path: str | Path,
    *,
    template_context: SqlTemplateContext | None = None,
) -> str:
    """Render a local ``.sql`` file with strict Jinja templating.

    Template context is for trusted SQL structure only, such as relation names,
    optional predicates, and generated placeholder lists. Runtime data values
    should still be passed through DuckDB parameters.
    """
    path = Path(sql_path)
    if path.suffix.lower() != ".sql":
        raise ValueError(f"SQL template path must end with .sql: {path}")
    return render_sql(
        path.read_text(encoding="utf-8"),
        template_context=template_context,
        template_name=str(path),
    )


def render_sql(
    sql: str,
    *,
    template_context: SqlTemplateContext | None = None,
    template_name: str = "<inline-sql>",
) -> str:
    """Render a SQL string with the same strict rules used for SQL files."""
    template = _jinja_environment().from_string(sql)
    template.name = template_name
    return template.render(dict(template_context or {})).strip()


@lru_cache(maxsize=1)
def _jinja_environment() -> Environment:
    """Return the shared SQL-template Jinja environment."""
    environment = Environment(
        autoescape=False,
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
        undefined=StrictUndefined,
    )
    environment.filters["identifier"] = _quote_identifier
    environment.filters["sql_literal"] = _sql_literal
    environment.globals["param"] = _parameter_marker
    return environment


def _quote_identifier(value: object) -> str:
    """Quote a DuckDB identifier for trusted schema, table, or column names."""
    text = str(value)
    if not text or "\x00" in text:
        raise ValueError(f"Invalid DuckDB identifier: {text!r}")
    return '"' + text.replace('"', '""') + '"'


def _sql_literal(value: object) -> str:
    """Render a DuckDB literal for trusted static template values."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float | Decimal):
        return str(value)
    if isinstance(value, date | datetime):
        return "'" + value.isoformat().replace("'", "''") + "'"
    return "'" + str(value).replace("'", "''") + "'"


def _parameter_marker() -> str:
    """Render one DuckDB parameter marker."""
    return "?"
