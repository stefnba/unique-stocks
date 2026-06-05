"""Tests for EOD price task helpers."""

from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from core.clients.lake import DataLakeClient
from core.ingestion import BronzeParseResult
from core.ingestion.coverage import (
    COVERAGE_STATUS_COMPLETED,
    COVERAGE_STATUS_NO_DATA,
    COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED,
    unit_key_hash,
)
from domains.eod_price import tasks
from domains.eod_price.coverage import (
    EOD_TICKER_BACKFILL_UNIT_TYPE,
    eod_ticker_backfill_unit_key,
)
from domains.eod_price.models import EODBar
from domains.instrument.universe import SilverIngestionContractError

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
        selection_coverage_rows: list[dict[str, object]] | None = None,
        tables: set[tuple[str, str]] | None = None,
    ) -> None:
        """Configure query results and table existence."""
        self.instrument_rows = instrument_rows or []
        self.price_tickers = price_tickers or []
        self.price_ranges = price_ranges or {ticker: (FROM_DATE, TO_DATE) for ticker in self.price_tickers}
        self.selection_coverage_rows = selection_coverage_rows or []
        self.coverage_rows: list[dict[str, object]] = []
        self.tables = tables or {
            ("silver", "int_eod_price_backfill_symbol_status"),
            ("silver", "int_eod_price_backfill_no_data_coverage"),
            ("silver", "int_eod_price_backfill_terminal_coverage"),
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
        if "silver.int_eod_price_backfill_symbol_status" in sql:
            pending, done, covered = self._pending_provider_symbols(params or [])
            return [
                {
                    "provider_symbol": symbol,
                    "total_symbols": len(self.instrument_rows),
                    "completed_price_symbols": len(done),
                    "terminal_no_data_symbols": len(covered),
                }
                for symbol in pending
            ]
        return []

    def _pending_provider_symbols(self, params: Sequence[Any]) -> tuple[list[str], set[str], set[str]]:
        """Evaluate the combined pending-symbol query for the fake lake."""
        requested_from: date | None = FROM_DATE
        requested_to = TO_DATE
        if len(params) >= 5:
            requested_from = date.fromisoformat(str(params[2])) if params[2] is not None else None
            requested_to = date.fromisoformat(str(params[4]))
        all_symbols = sorted(str(row["provider_symbol"]) for row in self.instrument_rows)
        completed_coverage = {
            str(row["provider_symbol"])
            for row in self.selection_coverage_rows
            if row.get("provider_exchange_code") == "US"
            and row.get("from_date") == requested_from
            and row.get("to_date") == requested_to
            and row.get("status") == COVERAGE_STATUS_COMPLETED
            and row.get("provider_symbol")
        }
        done = {
            ticker
            for ticker, (min_date, max_date) in self.price_ranges.items()
            if requested_from is not None and min_date <= requested_from and max_date >= requested_to
        }
        done.update(completed_coverage)
        covered = {
            str(row["provider_symbol"])
            for row in self.selection_coverage_rows
            if row.get("provider_exchange_code") == "US"
            and row.get("from_date") == requested_from
            and row.get("to_date") == requested_to
            and row.get("status", COVERAGE_STATUS_NO_DATA) == COVERAGE_STATUS_NO_DATA
            and row.get("provider_symbol")
        }
        pending = [symbol for symbol in all_symbols if symbol not in done and symbol not in covered]
        return pending, done, covered

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
    lake = FakeLake(
        instrument_rows=[
            {"provider_symbol": "AAPL.US"},
            {"provider_symbol": "MSFT.US"},
            {"provider_symbol": "DELIST.US"},
            {"provider_symbol": "WIDER.US"},
        ],
        price_tickers=["AAPL.US"],
        selection_coverage_rows=[
            {
                "provider_exchange_code": "US",
                "provider_symbol": "DELIST.US",
                "from_date": FROM_DATE,
                "to_date": TO_DATE,
            },
            {
                "provider_exchange_code": "US",
                "provider_symbol": "WIDER.US",
                "from_date": FROM_DATE,
                "to_date": date(2026, 6, 30),
            },
        ],
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    pending = tasks.load_backfill_pending_symbols.fn("US", FROM_DATE, TO_DATE)

    assert pending == ["MSFT.US", "WIDER.US"]
    sql, params = lake.queries[0]
    assert "silver.int_eod_price_backfill_symbol_status" in sql
    assert "silver.int_eod_price_backfill_terminal_coverage" in sql
    assert "status.data_provider = ?" in sql
    assert "coverage.status = 'completed'" in sql
    assert "COUNT(*) OVER () AS total_symbols" in sql
    assert params and params[8] == "eodhd"
    assert "unit_key_json" not in sql
    assert params == [
        "eodhd",
        "US",
        FROM_DATE.isoformat(),
        FROM_DATE.isoformat(),
        TO_DATE.isoformat(),
        FROM_DATE.isoformat(),
        FROM_DATE.isoformat(),
        TO_DATE.isoformat(),
        "eodhd",
        "US",
    ]


def test_load_backfill_pending_keeps_partial_price_history_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recent daily bar should not make a full historical backfill look complete."""
    lake = FakeLake(
        instrument_rows=[
            {"provider_symbol": "AAPL.US"},
            {"provider_symbol": "MSFT.US"},
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


def test_load_backfill_pending_full_history_requires_completed_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Open-start backfills should not treat daily-only Bronze rows as full history."""
    lake = FakeLake(
        instrument_rows=[
            {"provider_symbol": "AAPL.US"},
            {"provider_symbol": "MSFT.US"},
        ],
        price_ranges={
            "AAPL.US": (date(2026, 5, 31), date(2026, 5, 31)),
            "MSFT.US": (FROM_DATE, TO_DATE),
        },
        selection_coverage_rows=[
            {
                "provider_exchange_code": "US",
                "provider_symbol": "MSFT.US",
                "from_date": None,
                "to_date": TO_DATE,
                "status": COVERAGE_STATUS_COMPLETED,
            },
        ],
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    pending = tasks.load_backfill_pending_symbols.fn("US", None, TO_DATE)

    assert pending == ["AAPL.US"]
    _, params = lake.queries[0]
    assert params == [
        "eodhd",
        "US",
        None,
        None,
        TO_DATE.isoformat(),
        None,
        None,
        TO_DATE.isoformat(),
        "eodhd",
        "US",
    ]


def test_load_backfill_pending_ignores_provider_quota_deferred_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Quota-deferred audit rows stay pending because the no-data Silver view filters them out."""
    lake = FakeLake(
        instrument_rows=[{"provider_symbol": "MSFT.US"}],
        selection_coverage_rows=[],
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    pending = tasks.load_backfill_pending_symbols.fn("US", FROM_DATE, TO_DATE)

    assert pending == ["MSFT.US"]


def test_load_backfill_pending_requires_symbol_status_view(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backfill auto-selection should fail clearly when the dbt status contract is unavailable."""
    lake = FakeLake(
        tables={
            ("silver", "int_eod_price_backfill_terminal_coverage"),
            ("pipeline", "ingestion_coverage"),
        }
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    with pytest.raises(SilverIngestionContractError, match="int_eod_price_backfill_symbol_status"):
        tasks.load_backfill_pending_symbols.fn("US", FROM_DATE, TO_DATE)


def test_load_backfill_pending_requires_terminal_coverage_view(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backfill auto-selection should fail clearly when the dbt coverage contract is unavailable."""
    lake = FakeLake(tables={("silver", "int_eod_price_backfill_symbol_status"), ("pipeline", "ingestion_coverage")})
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    with pytest.raises(SilverIngestionContractError, match="int_eod_price_backfill_terminal_coverage"):
        tasks.load_backfill_pending_symbols.fn("US", FROM_DATE, TO_DATE)


def test_load_missing_eod_backfill_selection_views_reports_absent_contracts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backfill preflight should identify which Silver selector views need a build."""
    lake = FakeLake(tables={("silver", "int_eod_price_backfill_symbol_status")})
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    missing = tasks.load_missing_eod_backfill_selection_views.fn()

    assert missing == ["int_eod_price_backfill_terminal_coverage"]


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


def test_write_eod_completed_coverage_records_open_start_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """Successful full-history backfills should write completed coverage with from_date null."""
    lake = FakeLake(tables={("pipeline", "ingestion_coverage")})
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    result = tasks.write_eod_backfill_completed_coverage.fn(
        run_id="018f0000-0000-7000-8000-000000000004",
        provider_exchange_code="US",
        from_date=None,
        to_date=TO_DATE,
        outcomes=[
            {
                "ticker": "AAPL.US",
                "rows_raw": 2,
                "rows_valid": 2,
                "rows_rejected": 0,
                "source_uri": "s3://bucket/full-history.jsonl",
            }
        ],
    )

    assert result.rows_written == 1
    schema, table, records = lake.inserted[0]
    assert schema == "pipeline"
    assert table == "ingestion_coverage"
    record = records[0]
    assert record["status"] == COVERAGE_STATUS_COMPLETED
    assert record["unit_key_json"]["ticker"] == "AAPL.US"
    assert record["unit_key_json"]["from_date"] is None
    assert record["unit_key_json"]["to_date"] == TO_DATE.isoformat()


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


def test_write_backfill_eod_batch_ignores_existing_and_duplicate_bars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backfill batch writes should tolerate partial reruns and provider duplicate dates."""
    lake = DataLakeClient(connection_string=":memory:")
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    existing = _eod_source("0P0001OMXU.AS", date(2022, 2, 8))
    first = tasks.write_backfill_eod_batch.fn([existing], provider_exchange_code="AS")

    overlapping = [
        _eod_source("0P0001OMXU.AS", date(2022, 2, 8)),
        _eod_source("0P0001OMXU.AS", date(2022, 2, 9)),
        _eod_source("0P0001OMXU.AS", date(2022, 2, 9)),
    ]
    second = tasks.write_backfill_eod_batch.fn(overlapping, provider_exchange_code="AS")

    rows = lake.load(
        "eod_price",
        schema="bronze",
        columns=["ticker", "bar_date", "data_provider"],
        order_by="bar_date",
    )
    assert first.rows_written == 1
    assert second.rows_written == 1
    assert rows == [
        {"ticker": "0P0001OMXU.AS", "bar_date": date(2022, 2, 8), "data_provider": "eodhd"},
        {"ticker": "0P0001OMXU.AS", "bar_date": date(2022, 2, 9), "data_provider": "eodhd"},
    ]


def _eod_source(ticker: str, bar_date: date) -> BronzeParseResult[EODBar]:
    """Build a valid parsed EOD bar source."""
    return BronzeParseResult(
        row=EODBar(
            provider_exchange_code=ticker.rsplit(".", maxsplit=1)[1],
            ticker=ticker,
            bar_date=bar_date,
            open=Decimal("10"),
            high=Decimal("12"),
            low=Decimal("9"),
            close=Decimal("11"),
            volume=100,
            adjusted_close=Decimal("11"),
        ),
        raw_fragment={"date": bar_date.isoformat()},
    )
