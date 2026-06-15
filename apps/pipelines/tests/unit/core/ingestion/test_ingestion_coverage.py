"""Tests for generic pipeline.ingestion_coverage helpers."""

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import duckdb

from core.ingestion.coverage import (
    COVERAGE_STATUS_NO_DATA,
    ingestion_coverage_recorded,
    list_ingestion_coverage_unit_keys,
    record_ingestion_coverage,
    unit_key_hash,
)
from core.lake import DataLakeClient


class FakeLake:
    """Minimal lake fake for ingestion coverage."""

    def __init__(self) -> None:
        """Initialize empty coverage storage."""
        self.rows: list[dict[str, object]] = []
        self.queries: list[tuple[str, Sequence[Any] | None]] = []

    def qualified_name(self, schema: str, table: str) -> str:
        """Return a stable qualified name."""
        return f"{schema}.{table}"

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Return count of rows matching the idempotency key in params."""
        if "COUNT" not in sql or not params:
            return {"cnt": 0}
        _, _, _, key_hash, status = params
        count = sum(1 for row in self.rows if row["unit_key_hash"] == key_hash and row["status"] == status)
        return {"cnt": count}

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Return rows for list queries."""
        self.queries.append((sql, params))
        if "unit_key_json" not in sql or not params:
            return []
        domain, provider, unit_type, status = params[:4]
        return [
            {"unit_key_json": row["unit_key_json"]}
            for row in self.rows
            if row["domain"] == domain
            and row["provider"] == provider
            and row["unit_type"] == unit_type
            and row["status"] == status
        ]

    def insert_rows(self, schema: str, table: str, rows: list[dict[str, Any]]) -> int:
        """Append coverage rows."""
        self.rows.extend(rows)
        return len(rows)


def test_record_ingestion_coverage_is_idempotent() -> None:
    """Second insert for the same unit key and status is a no-op."""
    lake = FakeLake()
    unit_key = {"ticker": "AAPL.US", "from_date": "2026-05-01"}
    recorded_at = datetime(2026, 6, 2, tzinfo=UTC)

    first = record_ingestion_coverage(
        lake,
        run_id="018f0000-0000-7000-8000-000000000001",
        domain="eod_price",
        provider="eodhd",
        unit_type="ticker_backfill",
        unit_key=unit_key,
        status=COVERAGE_STATUS_NO_DATA,
        recorded_at=recorded_at,
    )
    second = record_ingestion_coverage(
        lake,
        run_id="018f0000-0000-7000-8000-000000000002",
        domain="eod_price",
        provider="eodhd",
        unit_type="ticker_backfill",
        unit_key=unit_key,
        status=COVERAGE_STATUS_NO_DATA,
        recorded_at=recorded_at,
    )

    assert first == 1
    assert second == 0
    assert len(lake.rows) == 1
    assert lake.rows[0]["unit_key_hash"] == unit_key_hash(unit_key)


def test_ingestion_coverage_recorded_matches_hash() -> None:
    """Recorded check uses unit_key_hash, not raw ticker string."""
    lake = FakeLake()
    unit_key = {"ticker": "DONE.US", "from_date": "2026-05-01"}
    lake.rows.append(
        {
            "domain": "eod_price",
            "provider": "eodhd",
            "unit_type": "ticker_backfill",
            "unit_key_hash": unit_key_hash(unit_key),
            "unit_key_json": unit_key,
            "status": COVERAGE_STATUS_NO_DATA,
        }
    )

    assert ingestion_coverage_recorded(
        lake,
        domain="eod_price",
        provider="eodhd",
        unit_type="ticker_backfill",
        unit_key=unit_key,
        status=COVERAGE_STATUS_NO_DATA,
    )
    assert not ingestion_coverage_recorded(
        lake,
        domain="eod_price",
        provider="eodhd",
        unit_type="ticker_backfill",
        unit_key={"ticker": "OTHER.US", "from_date": "2026-05-01"},
        status=COVERAGE_STATUS_NO_DATA,
    )


def test_list_ingestion_coverage_unit_keys_decodes_json_strings() -> None:
    """Coverage list helpers should handle DuckDB JSON values returned as strings."""
    lake = FakeLake()
    matching_key = {"ticker": "DONE.US", "from_date": "2026-05-01", "to_date": "2026-05-31"}
    other_key = {"ticker": "OTHER.US", "from_date": "2026-05-01", "to_date": "2026-06-30"}
    for unit_key in (matching_key, other_key):
        lake.rows.append(
            {
                "domain": "eod_price",
                "provider": "eodhd",
                "unit_type": "ticker_backfill",
                "unit_key_hash": unit_key_hash(unit_key),
                "unit_key_json": json.dumps(unit_key),
                "status": COVERAGE_STATUS_NO_DATA,
            }
        )

    keys = list_ingestion_coverage_unit_keys(
        lake,
        domain="eod_price",
        provider="eodhd",
        unit_type="ticker_backfill",
        status=COVERAGE_STATUS_NO_DATA,
        unit_key_matches={"from_date": "2026-05-01", "to_date": "2026-05-31"},
    )

    assert keys == [matching_key]


def test_list_ingestion_coverage_pushes_matches_into_sql() -> None:
    """Coverage list helpers should send JSON-field matches to the lake query."""
    lake = FakeLake()

    list_ingestion_coverage_unit_keys(
        lake,
        domain="eod_price",
        provider="eodhd",
        unit_type="ticker_backfill",
        status=COVERAGE_STATUS_NO_DATA,
        unit_key_matches={"provider_exchange_code": "US", "from_date": "2026-05-01"},
    )

    sql = lake.queries[-1][0]
    params = lake.queries[-1][1] or []
    assert "json_extract_string(unit_key_json, ?) = ?" in sql
    assert '$."provider_exchange_code"' in params
    assert "US" in params


def test_list_ingestion_coverage_filters_duckdb_json_in_sql() -> None:
    """Coverage list helpers should filter real DuckDB JSON columns before Python fallback."""
    lake = DataLakeClient(connection=duckdb.connect(":memory:"))
    lake.execute(
        """
        CREATE TABLE pipeline.ingestion_coverage (
            domain VARCHAR,
            provider VARCHAR,
            unit_type VARCHAR,
            status VARCHAR,
            unit_key_json JSON
        )
        """
    )
    matching_key = {"ticker": "DONE.US", "from_date": "2026-05-01", "to_date": "2026-05-31"}
    other_key = {"ticker": "OTHER.US", "from_date": "2026-05-01", "to_date": "2026-06-30"}
    for unit_key in (matching_key, other_key):
        lake.execute(
            """
            INSERT INTO pipeline.ingestion_coverage
            VALUES (?, ?, ?, ?, ?)
            """,
            ["eod_price", "eodhd", "ticker_backfill", COVERAGE_STATUS_NO_DATA, json.dumps(unit_key)],
        )

    keys = list_ingestion_coverage_unit_keys(
        lake,
        domain="eod_price",
        provider="eodhd",
        unit_type="ticker_backfill",
        status=COVERAGE_STATUS_NO_DATA,
        unit_key_matches={"from_date": "2026-05-01", "to_date": "2026-05-31"},
    )

    assert keys == [matching_key]
