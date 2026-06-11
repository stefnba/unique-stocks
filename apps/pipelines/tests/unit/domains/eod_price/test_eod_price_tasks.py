"""Tests for EOD price task helpers."""

from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from core.clients.lake import DataLakeClient
from core.clients.lake.sql import SqlTemplateContext, render_sql_file
from core.ingestion import BronzeParseResult
from core.ingestion.coverage import (
    COVERAGE_STATUS_COMPLETED,
    COVERAGE_STATUS_NO_DATA,
    COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED,
    unit_key_hash,
)
from domains.eod_price import tasks
from domains.eod_price.coverage import (
    EOD_INSTRUMENT_BACKFILL_UNIT_TYPE,
    eod_instrument_backfill_unit_key,
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
        day_coverage_rows: list[dict[str, object]] | None = None,
        selection_coverage_rows: list[dict[str, object]] | None = None,
        expected_exchange_days: int = 1,
        tables: set[tuple[str, str]] | None = None,
    ) -> None:
        """Configure query results and table existence."""
        self.instrument_rows = instrument_rows or []
        self.day_coverage_rows = day_coverage_rows or []
        self.selection_coverage_rows = selection_coverage_rows or []
        self.expected_exchange_days = expected_exchange_days
        self.coverage_rows: list[dict[str, object]] = []
        self.tables = tables or {
            ("silver", "int_latest_instrument_universe"),
            ("silver", "int_exchange_trading_day"),
            ("silver", "int_eod_price_instrument_day_coverage"),
            ("silver", "int_eod_price_exchange_day_status"),
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
        if "silver.int_eod_price_instrument_day_coverage" in sql:
            pending, completed, covered = self._pending_provider_instruments(params or [])
            return [
                {
                    "provider_instrument_code": instrument,
                    "expected_instrument_days": details["expected_days"],
                    "missing_price_days": details["missing_days"],
                    "unknown_calendar_days": details["unknown_days"],
                    "unknown_calendar_coverage_days": details["unknown_calendar_coverage_days"],
                    "unknown_instrument_lifecycle_days": details["unknown_lifecycle_days"],
                    "total_instruments": len(self.instrument_rows),
                    "completed_coverage_instruments": len(completed),
                    "terminal_no_data_instruments": len(covered),
                }
                for instrument, details in pending
            ]
        return []

    def query_file(
        self,
        sql_path: str | Path,
        params: Sequence[Any] | None = None,
        *,
        template_context: SqlTemplateContext | None = None,
    ) -> list[dict[str, Any]]:
        """Render a SQL file before dispatching to the fake query evaluator."""
        return self.query(render_sql_file(sql_path, template_context=template_context), params)

    def _pending_provider_instruments(
        self, params: Sequence[Any]
    ) -> tuple[list[tuple[str, dict[str, int]]], set[str], set[str]]:
        """Evaluate the combined pending-instrument query for the fake lake."""
        requested_from: date | None = FROM_DATE
        requested_to = TO_DATE
        if len(params) >= 18:
            requested_from = date.fromisoformat(str(params[14])) if params[14] is not None else None
            requested_to = date.fromisoformat(str(params[16]))
        all_instruments = sorted(str(row["provider_instrument_code"]) for row in self.instrument_rows)
        completed_coverage = {
            str(row["provider_instrument_code"])
            for row in self.selection_coverage_rows
            if row.get("provider_exchange_code") == "US"
            and row.get("from_date") == requested_from
            and row.get("to_date") == requested_to
            and row.get("status") == COVERAGE_STATUS_COMPLETED
            and row.get("provider_instrument_code")
        }
        covered = {
            str(row["provider_instrument_code"])
            for row in self.selection_coverage_rows
            if row.get("provider_exchange_code") == "US"
            and row.get("from_date") == requested_from
            and row.get("to_date") == requested_to
            and row.get("status", COVERAGE_STATUS_NO_DATA) == COVERAGE_STATUS_NO_DATA
            and row.get("provider_instrument_code")
        }
        coverage_by_instrument: dict[str, dict[str, int]] = {}
        for row in self.day_coverage_rows:
            if row.get("provider_exchange_code", "US") != "US":
                continue
            bar_date = row.get("bar_date", FROM_DATE)
            if not isinstance(bar_date, date):
                continue
            if bar_date > requested_to or (requested_from is not None and bar_date < requested_from):
                continue
            instrument = str(row["provider_instrument_code"])
            details = coverage_by_instrument.setdefault(
                instrument,
                {
                    "expected_days": 0,
                    "missing_days": 0,
                    "unknown_days": 0,
                    "unknown_calendar_coverage_days": 0,
                    "unknown_lifecycle_days": 0,
                    "has_observed": 0,
                    "has_lifecycle_evidence": 0,
                },
            )
            details["expected_days"] += 1
            if row.get("coverage_status") == "missing_price":
                details["missing_days"] += 1
            if row.get("coverage_status") == "unknown_calendar":
                details["unknown_days"] += 1
            if row.get("coverage_status") == "unknown_calendar_coverage":
                details["unknown_calendar_coverage_days"] += 1
            if row.get("coverage_status") == "unknown_instrument_lifecycle":
                details["unknown_lifecycle_days"] += 1
            if row.get("coverage_status") == "priced" or row.get("min_bar_date") is not None:
                details["has_observed"] = 1
            if (
                row.get("has_provider_lifecycle_evidence")
                or row.get("min_bar_date") is not None
                or row.get("coverage_status") == "priced"
            ):
                details["has_lifecycle_evidence"] = 1
        pending: list[tuple[str, dict[str, int]]] = []
        for instrument in all_instruments:
            details = coverage_by_instrument.get(
                instrument,
                {
                    "expected_days": 0,
                    "missing_days": 0,
                    "unknown_days": 0,
                    "unknown_calendar_coverage_days": 0,
                    "unknown_lifecycle_days": 0,
                    "has_observed": 0,
                    "has_lifecycle_evidence": 0,
                },
            )
            if self.expected_exchange_days <= 0:
                continue
            if instrument in completed_coverage or instrument in covered:
                continue
            if (
                requested_from is None
                or details["missing_days"] > 0
                or details["unknown_calendar_coverage_days"] > 0
                or details["unknown_lifecycle_days"] > 0
                or details["expected_days"] == 0
                or not details["has_observed"]
                or not details["has_lifecycle_evidence"]
            ):
                pending.append((instrument, details))
        return pending, completed_coverage, covered

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
    """Pending instruments omit complete coverage rows and exact-range terminal no_data coverage."""
    lake = FakeLake(
        instrument_rows=[
            {"provider_instrument_code": "AAPL"},
            {"provider_instrument_code": "MSFT"},
            {"provider_instrument_code": "DELIST"},
            {"provider_instrument_code": "WIDER"},
        ],
        day_coverage_rows=[
            {"provider_instrument_code": "AAPL", "bar_date": FROM_DATE, "coverage_status": "priced"},
            {"provider_instrument_code": "MSFT", "bar_date": FROM_DATE, "coverage_status": "missing_price"},
        ],
        selection_coverage_rows=[
            {
                "provider_exchange_code": "US",
                "provider_instrument_code": "DELIST",
                "from_date": FROM_DATE,
                "to_date": TO_DATE,
            },
            {
                "provider_exchange_code": "US",
                "provider_instrument_code": "WIDER",
                "from_date": FROM_DATE,
                "to_date": date(2026, 6, 30),
            },
        ],
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    pending = tasks.load_backfill_pending_instruments.fn("US", FROM_DATE, TO_DATE)

    assert pending == ["MSFT", "WIDER"]
    sql, params = lake.queries[0]
    assert "silver.int_latest_instrument_universe" in sql
    assert "silver.int_eod_price_instrument_day_coverage" in sql
    assert "silver.int_exchange_trading_day" in sql
    assert "silver.int_eod_price_backfill_terminal_coverage" in sql
    assert "coverage.status = 'completed'" in sql
    assert "coverage_status = 'missing_price'" in sql
    assert "coverage_status = 'unknown_calendar_coverage'" in sql
    assert "coverage_status = 'unknown_instrument_lifecycle'" in sql
    assert "coverage.status = 'completed'" in sql
    assert "COUNT(*) OVER () AS total_instruments" in sql
    assert params and params[12] == "eodhd"
    assert "unit_key_json" not in sql
    assert params == [
        "eodhd",
        "US",
        "eodhd",
        "US",
        TO_DATE.isoformat(),
        FROM_DATE.isoformat(),
        FROM_DATE.isoformat(),
        "eodhd",
        "US",
        TO_DATE.isoformat(),
        FROM_DATE.isoformat(),
        FROM_DATE.isoformat(),
        "eodhd",
        "US",
        FROM_DATE.isoformat(),
        FROM_DATE.isoformat(),
        TO_DATE.isoformat(),
        FROM_DATE.isoformat(),
    ]


def test_load_backfill_pending_keeps_partial_price_history_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recent daily bar should not make a full historical backfill look complete."""
    lake = FakeLake(
        instrument_rows=[
            {"provider_instrument_code": "AAPL"},
            {"provider_instrument_code": "MSFT"},
        ],
        day_coverage_rows=[
            {"provider_instrument_code": "AAPL", "bar_date": date(2026, 5, 15), "coverage_status": "missing_price"},
            {"provider_instrument_code": "MSFT", "bar_date": date(2026, 5, 15), "coverage_status": "priced"},
        ],
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    pending = tasks.load_backfill_pending_instruments.fn("US", FROM_DATE, TO_DATE)

    assert pending == ["AAPL"]


def test_load_backfill_pending_keeps_unknown_control_states_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calendar-horizon and lifecycle unknowns should remain planned work."""
    lake = FakeLake(
        instrument_rows=[
            {"provider_instrument_code": "HOLIDAY"},
            {"provider_instrument_code": "NEWIPO"},
        ],
        day_coverage_rows=[
            {
                "provider_instrument_code": "HOLIDAY",
                "bar_date": date(2026, 5, 15),
                "coverage_status": "unknown_calendar_coverage",
                "has_provider_lifecycle_evidence": True,
            },
            {
                "provider_instrument_code": "NEWIPO",
                "bar_date": date(2026, 5, 15),
                "coverage_status": "unknown_instrument_lifecycle",
            },
        ],
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    pending = tasks.load_backfill_pending_instruments.fn("US", FROM_DATE, TO_DATE)

    assert pending == ["HOLIDAY", "NEWIPO"]


def test_load_backfill_pending_keeps_partial_no_data_without_observed_history_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Partial no-data coverage should not satisfy an explicit no-price instrument window."""
    lake = FakeLake(
        instrument_rows=[
            {"provider_instrument_code": "AAPL"},
        ],
        day_coverage_rows=[
            {
                "provider_instrument_code": "AAPL",
                "bar_date": date(2026, 5, 15),
                "coverage_status": "known_no_data",
            },
        ],
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    pending = tasks.load_backfill_pending_instruments.fn("US", FROM_DATE, TO_DATE)

    assert pending == ["AAPL"]


def test_load_backfill_pending_full_history_requires_completed_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Open-start backfills should not treat daily-only Bronze rows as full history."""
    lake = FakeLake(
        instrument_rows=[
            {"provider_instrument_code": "AAPL"},
            {"provider_instrument_code": "MSFT"},
        ],
        day_coverage_rows=[
            {"provider_instrument_code": "AAPL", "bar_date": TO_DATE, "coverage_status": "priced"},
            {"provider_instrument_code": "MSFT", "bar_date": TO_DATE, "coverage_status": "priced"},
        ],
        selection_coverage_rows=[
            {
                "provider_exchange_code": "US",
                "provider_instrument_code": "MSFT",
                "from_date": None,
                "to_date": TO_DATE,
                "status": COVERAGE_STATUS_COMPLETED,
            },
        ],
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    pending = tasks.load_backfill_pending_instruments.fn("US", None, TO_DATE)

    assert pending == ["AAPL"]
    _, params = lake.queries[0]
    assert params == [
        "eodhd",
        "US",
        "eodhd",
        "US",
        TO_DATE.isoformat(),
        None,
        None,
        "eodhd",
        "US",
        TO_DATE.isoformat(),
        None,
        None,
        "eodhd",
        "US",
        None,
        None,
        TO_DATE.isoformat(),
        None,
    ]


def test_load_backfill_pending_ignores_provider_quota_deferred_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Quota-deferred audit rows stay pending because the no-data Silver view filters them out."""
    lake = FakeLake(
        instrument_rows=[{"provider_instrument_code": "MSFT"}],
        selection_coverage_rows=[],
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    pending = tasks.load_backfill_pending_instruments.fn("US", FROM_DATE, TO_DATE)

    assert pending == ["MSFT"]


def test_load_backfill_pending_requires_instrument_status_view(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backfill auto-selection should fail clearly when the dbt coverage contract is unavailable."""
    lake = FakeLake(
        tables={
            ("silver", "int_latest_instrument_universe"),
            ("silver", "int_exchange_trading_day"),
            ("silver", "int_eod_price_backfill_terminal_coverage"),
            ("pipeline", "ingestion_coverage"),
        }
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    with pytest.raises(SilverIngestionContractError, match="int_eod_price_instrument_day_coverage"):
        tasks.load_backfill_pending_instruments.fn("US", FROM_DATE, TO_DATE)


def test_load_backfill_pending_requires_terminal_coverage_view(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backfill auto-selection should fail clearly when the dbt coverage contract is unavailable."""
    lake = FakeLake(
        tables={
            ("silver", "int_latest_instrument_universe"),
            ("silver", "int_exchange_trading_day"),
            ("silver", "int_eod_price_instrument_day_coverage"),
            ("pipeline", "ingestion_coverage"),
        }
    )
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    with pytest.raises(SilverIngestionContractError, match="int_eod_price_backfill_terminal_coverage"):
        tasks.load_backfill_pending_instruments.fn("US", FROM_DATE, TO_DATE)


def test_load_missing_eod_backfill_selection_views_reports_absent_contracts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backfill preflight should identify which Silver selector views need a build."""
    lake = FakeLake(tables={("silver", "int_latest_instrument_universe")})
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    missing = tasks.load_missing_eod_backfill_selection_views.fn()

    assert missing == [
        "int_exchange_trading_day",
        "int_eod_price_instrument_day_coverage",
        "int_eod_price_backfill_terminal_coverage",
    ]


def test_query_exchange_day_coverage_gaps_by_explicit_pairs_renders_sql_file() -> None:
    """Explicit exchange/date repairs should render pair predicates and preserve param order."""
    lake = FakeLake()

    tasks._query_exchange_day_coverage_gaps(
        lake,
        status_q="silver.int_eod_price_exchange_day_status",
        provider_exchange_codes=[],
        from_date=None,
        to_date=None,
        exchange_dates={"US": FROM_DATE, "LSE": TO_DATE},
    )

    sql, params = lake.queries[0]
    assert "FROM silver.int_eod_price_exchange_day_status" in sql
    assert "(provider_exchange_code = ? AND bar_date = ?) OR (provider_exchange_code = ? AND bar_date = ?)" in sql
    assert params == ["eodhd", "LSE", TO_DATE.isoformat(), "US", FROM_DATE.isoformat()]


def test_query_exchange_day_coverage_gaps_by_codes_renders_optional_dates() -> None:
    """Exchange-code repairs should render IN placeholders and optional date filters."""
    lake = FakeLake()

    tasks._query_exchange_day_coverage_gaps(
        lake,
        status_q="silver.int_eod_price_exchange_day_status",
        provider_exchange_codes=["US", "LSE", "US"],
        from_date=FROM_DATE,
        to_date=TO_DATE,
        exchange_dates=None,
    )

    sql, params = lake.queries[0]
    assert "provider_exchange_code IN (?, ?)" in sql
    assert "AND bar_date >= ?" in sql
    assert "AND bar_date <= ?" in sql
    assert params == ["eodhd", "LSE", "US", FROM_DATE.isoformat(), TO_DATE.isoformat()]


def test_write_eod_backfill_coverage_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Coverage writes insert once per backfill partition."""
    lake = FakeLake(tables={("pipeline", "ingestion_coverage")})
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    first = tasks.write_eod_backfill_coverage.fn(
        run_id="018f0000-0000-7000-8000-000000000001",
        provider_exchange_code="US",
        provider_instrument_code="AAPL",
        from_date=FROM_DATE,
        to_date=TO_DATE,
        rows_raw=0,
        rows_valid=0,
        rows_rejected=0,
        source_uri="s3://bucket/landing.jsonl",
    )
    done_key = eod_instrument_backfill_unit_key(
        provider_exchange_code="US",
        provider_instrument_code="DONE",
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
        provider_instrument_code="DONE",
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
    assert record["unit_type"] == EOD_INSTRUMENT_BACKFILL_UNIT_TYPE
    assert record["unit_key_json"]["provider_instrument_code"] == "AAPL"


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
                "provider_instrument_code": "AAPL",
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
    assert record["unit_key_json"]["provider_instrument_code"] == "AAPL"
    assert record["unit_key_json"]["from_date"] is None
    assert record["unit_key_json"]["to_date"] == TO_DATE.isoformat()


def test_write_eod_deferred_coverage_records_unsubmitted_instruments(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provider-deferred backfill units are audited without marking them no_data."""
    lake = FakeLake(tables={("pipeline", "ingestion_coverage")})
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    result = tasks.write_eod_backfill_deferred_coverage.fn(
        run_id="018f0000-0000-7000-8000-000000000003",
        provider_exchange_code="US",
        provider_instrument_codes=["MSFT", "GOOG"],
        from_date=FROM_DATE,
        to_date=TO_DATE,
        reason="provider_rate_limited",
    )

    assert result.rows_written == 2
    assert {(schema, table) for schema, table, _ in lake.inserted} == {("pipeline", "ingestion_coverage")}
    records = [record for _, _, batch in lake.inserted for record in batch]
    assert {record["status"] for record in records} == {COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED}
    assert {record["unit_key_json"]["provider_instrument_code"] for record in records} == {"MSFT", "GOOG"}


def test_write_backfill_eod_batch_ignores_existing_and_duplicate_bars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backfill batch writes should tolerate partial reruns and provider duplicate dates."""
    lake = DataLakeClient(connection_string=":memory:")
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    existing = _eod_source("AS", "0P0001OMXU", date(2022, 2, 8))
    first = tasks.write_backfill_eod_batch.fn([existing], provider_exchange_code="AS")

    overlapping = [
        _eod_source("AS", "0P0001OMXU", date(2022, 2, 8)),
        _eod_source("AS", "0P0001OMXU", date(2022, 2, 9)),
        _eod_source("AS", "0P0001OMXU", date(2022, 2, 9)),
    ]
    second = tasks.write_backfill_eod_batch.fn(overlapping, provider_exchange_code="AS")

    rows = lake.load(
        "eod_price",
        schema="bronze",
        columns=["provider_exchange_code", "provider_instrument_code", "bar_date", "data_provider"],
        order_by="bar_date",
    )
    assert first.rows_written == 1
    assert second.rows_written == 1
    assert rows == [
        {
            "provider_exchange_code": "AS",
            "provider_instrument_code": "0P0001OMXU",
            "bar_date": date(2022, 2, 8),
            "data_provider": "eodhd",
        },
        {
            "provider_exchange_code": "AS",
            "provider_instrument_code": "0P0001OMXU",
            "bar_date": date(2022, 2, 9),
            "data_provider": "eodhd",
        },
    ]


def test_daily_bulk_write_ignores_historical_overlap_without_skipping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Historical rows for one date should not make the daily bulk partition look complete."""
    lake = DataLakeClient(connection_string=":memory:")
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    historical = _eod_source("AS", "0P0001OMXU", date(2022, 2, 8))
    backfill = tasks.write_backfill_eod_batch.fn([historical], provider_exchange_code="AS")
    already_after_backfill = tasks.eod_price_already_ingested.fn("AS", date(2022, 2, 8))

    daily_sources = [
        _eod_source("AS", "0P0001OMXU", date(2022, 2, 8), ingestion_mode="daily_bulk"),
        _eod_source("AS", "DAILYONLY", date(2022, 2, 8), ingestion_mode="daily_bulk"),
    ]
    daily = tasks.write_bronze_eod_price.fn(
        daily_sources,
        provider_exchange_code="AS",
        bar_date=date(2022, 2, 8),
        source_uri="s3://bucket/daily.jsonl",
    )
    already_after_daily = tasks.eod_price_already_ingested.fn("AS", date(2022, 2, 8))

    rows = lake.load(
        "eod_price",
        schema="bronze",
        columns=["provider_instrument_code", "ingestion_mode", "source_uri"],
        order_by="provider_instrument_code",
    )
    assert backfill.rows_written == 1
    assert already_after_backfill is False
    assert daily.rows_written == 1
    assert daily.reason is None
    assert already_after_daily is True
    assert rows == [
        {
            "provider_instrument_code": "0P0001OMXU",
            "ingestion_mode": "historical_backfill",
            "source_uri": None,
        },
        {
            "provider_instrument_code": "DAILYONLY",
            "ingestion_mode": "daily_bulk",
            "source_uri": "s3://bucket/daily.jsonl",
        },
    ]


def test_daily_already_ingested_allows_retry_when_coverage_has_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A partial exchange/date should be retryable after dbt coverage marks it missing."""
    lake = DataLakeClient(connection_string=":memory:")
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    daily = _eod_source("US", "AAPL", TO_DATE, ingestion_mode="daily_bulk")
    write = tasks.write_bronze_eod_price.fn([daily], provider_exchange_code="US", bar_date=TO_DATE)
    before_gate = tasks.eod_price_already_ingested.fn("US", TO_DATE)

    lake.execute(
        """
        CREATE TABLE silver.int_eod_price_exchange_day_status (
            data_provider VARCHAR,
            provider_exchange_code VARCHAR,
            bar_date DATE,
            exchange_day_status VARCHAR
        )
        """
    )
    lake.execute(
        """
        INSERT INTO silver.int_eod_price_exchange_day_status VALUES (?, ?, ?, ?)
        """,
        ["eodhd", "US", TO_DATE.isoformat(), "missing_price"],
    )
    after_gate = tasks.eod_price_already_ingested.fn("US", TO_DATE)

    assert write.rows_written == 1
    assert before_gate is True
    assert after_gate is False


def test_daily_bulk_write_repairs_missing_symbols_when_partition_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A partial daily partition should still insert missing provider instruments."""
    lake = DataLakeClient(connection_string=":memory:")
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    first = _eod_source("US", "AAPL", TO_DATE, ingestion_mode="daily_bulk")
    repair = [
        _eod_source("US", "AAPL", TO_DATE, ingestion_mode="daily_bulk"),
        _eod_source("US", "MSFT", TO_DATE, ingestion_mode="daily_bulk"),
    ]

    first_write = tasks.write_bronze_eod_price.fn([first], provider_exchange_code="US", bar_date=TO_DATE)
    repair_write = tasks.write_bronze_eod_price.fn(repair, provider_exchange_code="US", bar_date=TO_DATE)

    rows = lake.load(
        "eod_price",
        schema="bronze",
        columns=["provider_instrument_code", "ingestion_mode"],
        order_by="provider_instrument_code",
    )
    assert first_write.rows_written == 1
    assert repair_write.rows_written == 1
    assert repair_write.reason is None
    assert rows == [
        {"provider_instrument_code": "AAPL", "ingestion_mode": "daily_bulk"},
        {"provider_instrument_code": "MSFT", "ingestion_mode": "daily_bulk"},
    ]


def _eod_source(
    provider_exchange_code: str,
    provider_instrument_code: str,
    bar_date: date,
    ingestion_mode: str = "historical_backfill",
) -> BronzeParseResult[EODBar]:
    """Build a valid parsed EOD bar source."""
    return BronzeParseResult(
        row=EODBar(
            provider_exchange_code=provider_exchange_code,
            provider_instrument_code=provider_instrument_code,
            bar_date=bar_date,
            ingestion_mode=ingestion_mode,
            open=Decimal("10"),
            high=Decimal("12"),
            low=Decimal("9"),
            close=Decimal("11"),
            volume=100,
            adjusted_close=Decimal("11"),
        ),
        raw_fragment={"date": bar_date.isoformat()},
    )
