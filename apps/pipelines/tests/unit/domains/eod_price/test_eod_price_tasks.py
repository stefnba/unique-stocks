"""Tests for EOD price task helpers."""

import json
from collections.abc import Sequence
from datetime import date
from typing import Any

import pytest

from core.ingestion.coverage import COVERAGE_STATUS_NO_DATA, COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED, unit_key_hash
from domains.eod_price import tasks
from domains.eod_price.coverage import (
    EOD_PRICE_DOMAIN,
    EOD_TICKER_BACKFILL_UNIT_TYPE,
    eod_ticker_backfill_unit_key,
)

FROM_DATE = date(2026, 5, 1)
TO_DATE = date(2026, 5, 31)


class FakeLake:
    """Minimal lake fake for backfill pending and coverage writes."""

    def __init__(
        self,
        *,
        instrument_rows: list[dict[str, str]] | None = None,
        price_tickers: list[str] | None = None,
        price_ranges: dict[str, tuple[date, date]] | None = None,
        coverage_unit_keys: list[dict[str, object] | str] | None = None,
        tables: set[tuple[str, str]] | None = None,
    ) -> None:
        """Configure query results and table existence."""
        self.instrument_rows = instrument_rows or []
        self.price_tickers = price_tickers or []
        self.price_ranges = price_ranges or {ticker: (FROM_DATE, TO_DATE) for ticker in self.price_tickers}
        self.coverage_unit_keys = coverage_unit_keys or []
        self.coverage_rows: list[dict[str, object]] = []
        self.tables = tables or {
            ("bronze", "instrument"),
            ("bronze", "eod_price"),
            ("pipeline", "ingestion_coverage"),
        }
        self.inserted: list[tuple[str, str, list[dict[str, Any]]]] = []
        self.queries: list[tuple[str, Sequence[Any] | None]] = []

    def table_exists(self, schema: str, table: str) -> bool:
        """Return whether the fake exposes a table."""
        return (schema, table) in self.tables

    def qualified_name(self, schema: str, table: str) -> str:
        """Return a stable qualified name."""
        return f"{schema}.{table}"

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Return rows based on which table the SQL targets."""
        self.queries.append((sql, params))
        if "bronze.instrument" in sql:
            return self.instrument_rows
        if "bronze.eod_price" in sql:
            requested_from = date.fromisoformat(str((params or [None, None, FROM_DATE])[2]))
            requested_to = date.fromisoformat(str((params or [None, None, FROM_DATE, TO_DATE])[3]))
            return [
                {"ticker": ticker}
                for ticker, (min_date, max_date) in self.price_ranges.items()
                if min_date <= requested_from and max_date >= requested_to
            ]
        if "pipeline.ingestion_coverage" in sql:
            return [{"unit_key_json": key} for key in self.coverage_unit_keys]
        return []

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Return a single-row query result."""
        if "COUNT" not in sql or not params:
            return {"cnt": 0}
        _, _, _, key_hash, status = params
        count = sum(
            1 for row in self.coverage_rows if row.get("unit_key_hash") == key_hash and row.get("status") == status
        )
        return {"cnt": count}

    def insert_rows(self, schema: str, table: str, records: list[dict[str, Any]]) -> int:
        """Capture inserted pipeline rows."""
        self.inserted.append((schema, table, records))
        self.coverage_rows.extend(records)
        return len(records)


def test_load_backfill_pending_excludes_price_and_no_data_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pending symbols omit price rows and exact-range terminal no_data coverage."""
    delist_key = eod_ticker_backfill_unit_key(
        provider_exchange_code="US",
        ticker="DELIST.US",
        from_date=FROM_DATE,
        to_date=TO_DATE,
    )
    wider_range_key = eod_ticker_backfill_unit_key(
        provider_exchange_code="US",
        ticker="WIDER.US",
        from_date=FROM_DATE,
        to_date=date(2026, 6, 30),
    )
    lake = FakeLake(
        instrument_rows=[
            {"ticker": "AAPL"},
            {"ticker": "MSFT"},
            {"ticker": "DELIST"},
            {"ticker": "WIDER"},
        ],
        price_tickers=["AAPL.US"],
        coverage_unit_keys=[json.dumps(delist_key), wider_range_key],
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    pending = tasks.load_backfill_pending_symbols.fn("US", FROM_DATE, TO_DATE)

    assert pending == ["MSFT.US", "WIDER.US"]
    coverage_query = next(sql for sql, _ in lake.queries if "ingestion_coverage" in sql)
    assert "unit_key_json" in coverage_query
    assert "json_extract_string(unit_key_json, ?) = ?" in coverage_query
    assert any(
        params
        and params[0] == EOD_PRICE_DOMAIN
        and params[2] == EOD_TICKER_BACKFILL_UNIT_TYPE
        and params[3] == COVERAGE_STATUS_NO_DATA
        and '$."to_date"' in params
        and TO_DATE.isoformat() in params
        for _, params in lake.queries
    )


def test_load_backfill_pending_keeps_partial_price_history_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recent daily bar should not make a full historical backfill look complete."""
    lake = FakeLake(
        instrument_rows=[
            {"ticker": "AAPL"},
            {"ticker": "MSFT"},
        ],
        price_ranges={
            "AAPL.US": (date(2026, 5, 31), date(2026, 5, 31)),
            "MSFT.US": (FROM_DATE, TO_DATE),
        },
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    pending = tasks.load_backfill_pending_symbols.fn("US", FROM_DATE, TO_DATE)

    assert pending == ["AAPL.US"]


def test_write_eod_backfill_coverage_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Coverage writes insert once per backfill partition."""
    lake = FakeLake(tables={("pipeline", "ingestion_coverage")})
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    first = tasks.write_eod_backfill_coverage.fn(
        run_id="018f0000-0000-7000-8000-000000000001",
        provider_exchange_code="US",
        ticker="AAPL.US",
        from_date=FROM_DATE,
        to_date=TO_DATE,
        rows_raw=0,
        rows_valid=0,
        rows_rejected=0,
        source_uri="s3://bucket/landing.jsonl",
    )
    done_key = eod_ticker_backfill_unit_key(
        provider_exchange_code="US",
        ticker="DONE.US",
        from_date=FROM_DATE,
        to_date=TO_DATE,
    )
    lake.coverage_rows.append(
        {
            "unit_key_hash": unit_key_hash(done_key),
            "status": COVERAGE_STATUS_NO_DATA,
        }
    )
    second = tasks.write_eod_backfill_coverage.fn(
        run_id="018f0000-0000-7000-8000-000000000002",
        provider_exchange_code="US",
        ticker="DONE.US",
        from_date=FROM_DATE,
        to_date=TO_DATE,
        rows_raw=0,
        rows_valid=0,
        rows_rejected=0,
        source_uri="s3://bucket/done.jsonl",
    )

    assert first.rows_written == 1
    assert second.rows_written == 0
    assert second.reason == "already_recorded"
    assert len(lake.inserted) == 1
    schema, table, records = lake.inserted[0]
    assert schema == "pipeline"
    assert table == "ingestion_coverage"
    record = records[0]
    assert record["status"] == COVERAGE_STATUS_NO_DATA
    assert record["unit_type"] == EOD_TICKER_BACKFILL_UNIT_TYPE
    assert record["unit_key_json"]["ticker"] == "AAPL.US"


def test_write_eod_deferred_coverage_records_unsubmitted_tickers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provider-deferred backfill units are audited without marking them no_data."""
    lake = FakeLake(tables={("pipeline", "ingestion_coverage")})
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    result = tasks.write_eod_backfill_deferred_coverage.fn(
        run_id="018f0000-0000-7000-8000-000000000003",
        provider_exchange_code="US",
        tickers=["MSFT.US", "GOOG.US"],
        from_date=FROM_DATE,
        to_date=TO_DATE,
        reason="provider_rate_limited",
    )

    assert result.rows_written == 2
    assert {(schema, table) for schema, table, _ in lake.inserted} == {("pipeline", "ingestion_coverage")}
    records = [record for _, _, batch in lake.inserted for record in batch]
    assert {record["status"] for record in records} == {COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED}
    assert {record["unit_key_json"]["ticker"] for record in records} == {"MSFT.US", "GOOG.US"}
