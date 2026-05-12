"""
DuckDB / MotherDuck helpers.

All reads and writes go through this module. Pipeline code never imports duckdb directly.

Connection strategy:
  - MOTHERDUCK_TOKEN blank  → local file  "unique_stocks.db"   (dev)
  - MOTHERDUCK_TOKEN set    → "md:unique_stocks?motherduck_token=..."  (prod)

The connection is a module-level singleton created lazily on first use and
reused across tasks in the same process. DuckDB is not thread-safe for writes
from multiple threads on the same connection, but Prefect tasks in a single
worker process are sequential by default, so this is fine.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date
from typing import Any, Iterator

import duckdb
import structlog

log = structlog.get_logger(__name__)

_conn: duckdb.DuckDBPyConnection | None = None


def _get_connection() -> duckdb.DuckDBPyConnection:
    global _conn
    if _conn is None:
        from shared.config import settings

        conn_str = settings.duckdb_connection_string
        log.info("lake.connecting", connection=conn_str.split("?")[0])
        _conn = duckdb.connect(conn_str)
        _ensure_schemas(_conn)
    return _conn


def _ensure_schemas(conn: duckdb.DuckDBPyConnection) -> None:
    """Create schemas if they don't exist. Safe to call on every startup."""
    for schema in ("bronze", "silver", "gold", "pipeline"):
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def execute(sql: str, params: list[Any] | None = None) -> None:
    """Run a write statement (INSERT, CREATE, etc.) with no return value."""
    conn = _get_connection()
    if params:
        conn.execute(sql, params)
    else:
        conn.execute(sql)


def query(sql: str, params: list[Any] | None = None) -> list[dict]:
    """Run a SELECT and return rows as a list of dicts."""
    conn = _get_connection()
    rel = conn.execute(sql, params or [])
    columns = [desc[0] for desc in rel.description]
    return [dict(zip(columns, row)) for row in rel.fetchall()]


def query_one(sql: str, params: list[Any] | None = None) -> dict | None:
    """Run a SELECT and return the first row as a dict, or None."""
    rows = query(sql, params)
    return rows[0] if rows else None


def table_exists(schema: str, table: str) -> bool:
    row = query_one(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    )
    return row is not None


def insert_rows(schema: str, table: str, rows: list[dict]) -> int:
    """
    Bulk-insert a list of dicts into schema.table.
    Columns are inferred from the first row's keys — all rows must have the same keys.
    Returns number of rows inserted.
    """
    if not rows:
        return 0

    conn = _get_connection()
    columns = list(rows[0].keys())
    col_names = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO {schema}.{table} ({col_names}) VALUES ({placeholders})"

    values = [[r[c] for c in columns] for r in rows]
    conn.executemany(sql, values)
    log.info("lake.inserted", schema=schema, table=table, rows=len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Bronze-specific helpers
# ---------------------------------------------------------------------------


def already_ingested_dates(table: str, ticker: str, start: date, end: date) -> set[date]:
    """
    Return the set of bar_dates that already exist in bronze for a given ticker.
    Used for idempotency checks before fetching from the API.
    """
    rows = query(
        f"SELECT DISTINCT bar_date FROM bronze.{table} WHERE ticker = ? AND bar_date BETWEEN ? AND ?",
        [ticker, start.isoformat(), end.isoformat()],
    )
    return {r["bar_date"] for r in rows}


def already_ingested_exchange_date(table: str, exchange: str, bar_date: date) -> bool:
    """
    Return True if we already have bulk data for (exchange, bar_date) in bronze.
    Used for the bulk EOD prices idempotency check.
    """
    row = query_one(
        f"SELECT COUNT(*) AS cnt FROM bronze.{table} WHERE ticker LIKE ? AND bar_date = ?",
        [f"%.{exchange}", bar_date.isoformat()],
    )
    return bool(row and row["cnt"] > 0)


# ---------------------------------------------------------------------------
# Pipeline run tracking
# ---------------------------------------------------------------------------


def record_run_start(flow_name: str) -> str:
    """Insert a pipeline.runs row and return the run_id."""
    import uuid

    run_id = str(uuid.uuid4())
    execute(
        """
        INSERT INTO pipeline.runs (run_id, flow_name, status, started_at)
        VALUES (?, ?, 'running', now())
        """,
        [run_id, flow_name],
    )
    return run_id


def record_run_complete(run_id: str, rows_written: int) -> None:
    execute(
        """
        UPDATE pipeline.runs
        SET status = 'completed', completed_at = now(), rows_written = ?
        WHERE run_id = ?
        """,
        [rows_written, run_id],
    )


def record_run_failed(run_id: str, error: str) -> None:
    execute(
        """
        UPDATE pipeline.runs
        SET status = 'failed', completed_at = now(), error_message = ?
        WHERE run_id = ?
        """,
        [error[:2000], run_id],
    )
