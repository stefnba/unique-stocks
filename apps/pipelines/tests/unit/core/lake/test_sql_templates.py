"""Tests for file-backed lake SQL helpers."""

from datetime import date
from pathlib import Path

import duckdb
import pytest
from jinja2 import UndefinedError

from core.lake import DataLakeClient, render_sql_file


def test_query_file_renders_trusted_relation_and_keeps_value_params(tmp_path: Path) -> None:
    """SQL files should render structure while values stay as DuckDB params."""
    sql_path = tmp_path / "select_prices.sql"
    sql_path.write_text(
        """
        SELECT symbol, trade_date
        FROM {{ prices_relation }}
        WHERE trade_date = {{ param() }}
        ORDER BY symbol
        """,
        encoding="utf-8",
    )
    lake = DataLakeClient(connection=duckdb.connect(":memory:"))
    lake.execute("CREATE TABLE bronze.prices (symbol VARCHAR, trade_date DATE)")
    lake.execute("INSERT INTO bronze.prices VALUES (?, ?), (?, ?)", ["MSFT", "2026-05-01", "AAPL", "2026-05-01"])

    rows = lake.query_file(
        sql_path,
        [date(2026, 5, 1).isoformat()],
        template_context={"prices_relation": lake.qualified_name("bronze", "prices")},
    )

    assert rows == [
        {"symbol": "AAPL", "trade_date": date(2026, 5, 1)},
        {"symbol": "MSFT", "trade_date": date(2026, 5, 1)},
    ]


def test_render_sql_file_uses_strict_template_variables(tmp_path: Path) -> None:
    """Typos in SQL template context should fail before a query reaches DuckDB."""
    sql_path = tmp_path / "missing_relation.sql"
    sql_path.write_text("SELECT * FROM {{ missing_relation }}", encoding="utf-8")

    with pytest.raises(UndefinedError, match="missing_relation"):
        render_sql_file(sql_path)


def test_execute_file_runs_rendered_write_statement(tmp_path: Path) -> None:
    """Write statements can use the same SQL-file rendering path."""
    sql_path = tmp_path / "insert_price.sql"
    sql_path.write_text(
        "INSERT INTO {{ prices_relation }} VALUES ({{ param() }}, {{ param() }})",
        encoding="utf-8",
    )
    lake = DataLakeClient(connection=duckdb.connect(":memory:"))
    lake.execute("CREATE TABLE bronze.prices (symbol VARCHAR, trade_date DATE)")

    lake.execute_file(
        sql_path,
        ["AAPL", "2026-05-01"],
        template_context={"prices_relation": lake.qualified_name("bronze", "prices")},
    )

    assert lake.query("SELECT symbol, trade_date FROM bronze.prices") == [
        {"symbol": "AAPL", "trade_date": date(2026, 5, 1)}
    ]


def test_render_sql_file_supports_parameter_marker_macro(tmp_path: Path) -> None:
    """Runtime SQL rendering should turn ``param()`` into DuckDB markers."""
    sql_path = tmp_path / "with_param.sql"
    sql_path.write_text("SELECT {{ param() }} AS value", encoding="utf-8")

    assert render_sql_file(sql_path) == "SELECT ? AS value"
