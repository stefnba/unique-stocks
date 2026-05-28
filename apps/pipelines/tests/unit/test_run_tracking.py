"""Tests for pipeline audit tracking helpers."""

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import pytest

from core.ingestion.landing import LandingWrite
from core.ingestion.run_tracking import (
    LandingObjectRecord,
    PipelineRunTracker,
    RejectionRecord,
    RunCounters,
    RunUnitRecord,
    RunUnitTally,
    terminal_status,
)


class FakeLake:
    """Minimal lake fake for tracker tests."""

    inserted: list[tuple[str, str, list[dict[str, Any]]]]
    executed: list[tuple[str, Sequence[Any] | None]]

    def __init__(self) -> None:
        """Create an empty fake lake."""
        self.inserted = []
        self.executed = []

    def insert_rows(self, schema: str, table: str, rows: Iterable[Mapping[str, Any]]) -> int:
        """Capture inserted rows."""
        saved_rows = [dict(row) for row in rows]
        self.inserted.append((schema, table, saved_rows))
        return len(saved_rows)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """Capture executed statements."""
        self.executed.append((sql, params))

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Capture SELECT statements."""
        self.executed.append((sql, params))
        return []


def test_tracker_records_run_unit_and_landing_object() -> None:
    """Tracker rows should include stable ids, JSON keys, counters, and lineage."""
    lake = FakeLake()
    tracker = PipelineRunTracker(lake)  # type: ignore[arg-type]

    run_id = tracker.start_run(
        flow_name="eod-price-daily",
        domain="eod_price",
        run_kind="daily",
        provider="eodhd",
        parameters={"trade_date": "2026-05-22"},
    )
    unit_id = tracker.record_unit(
        run_id=run_id,
        domain="eod_price",
        provider="eodhd",
        unit_type="exchange_date",
        unit_key={"provider_exchange_code": "US", "bar_date": "2026-05-22"},
        status="completed",
        source_uri="s3://bucket/key.jsonl",
        rows_raw=100,
        rows_valid=99,
        rows_rejected=1,
        rows_written=99,
    )
    tracker.record_landing_object(
        run_id=run_id,
        unit_id=unit_id,
        domain="eod_price",
        provider="eodhd",
        dataset="eod_price.daily",
        source_uri="s3://bucket/key.jsonl",
        partition={"provider_exchange_code": "US", "bar_date": "2026-05-22"},
        rows_raw=100,
    )
    tracker.record_rejection(
        run_id=run_id,
        unit_id=unit_id,
        domain="eod_price",
        entity_key={"ticker": "BAD.US"},
        raw_fragment={"code": "BAD", "close": None},
        reason="parse_error",
        error=ValueError("bad close"),
    )
    tracker.complete_run(
        run_id,
        status="partial",
        counters=RunCounters(units_total=1, units_succeeded=1, rows_rejected=1, rows_written=99),
        summary={"failed": []},
    )

    assert [item[1] for item in lake.inserted] == ["runs", "run_units", "landing_objects", "rejections"]
    assert lake.inserted[0][2][0]["run_id"] == run_id
    assert lake.inserted[1][2][0]["unit_key_hash"]
    assert lake.inserted[1][2][0]["rows_rejected"] == 1
    assert lake.inserted[2][2][0]["unit_id"] == unit_id
    assert lake.inserted[3][2][0]["raw_hash"]
    assert lake.inserted[3][2][0]["error_class"] == "ValueError"
    assert lake.executed[-1][1] is not None
    assert lake.executed[-1][1][0] == "partial"


def test_terminal_status_marks_partial_for_failures_or_rejections() -> None:
    """Aggregate terminal status should distinguish clean and partial success."""
    assert terminal_status() == "completed"
    assert terminal_status(failed=1) == "partial"
    assert terminal_status(rejected=1) == "partial"
    assert terminal_status(skipped_all=True) == "skipped"


def test_run_unit_tally_counts_recorded_unit_statuses() -> None:
    """Run unit tallies should mirror the run_units status model."""
    tally = RunUnitTally()

    tally.record("completed")
    tally.record("failed")
    tally.record("skipped")
    tally.record("unsupported")
    counters = tally.counters(rows_raw=10, rows_written=7)

    assert counters.units_total == 4
    assert counters.units_succeeded == 1
    assert counters.units_failed == 1
    assert counters.units_skipped == 2
    assert counters.rows_raw == 10
    assert counters.rows_written == 7


def test_track_run_fails_on_exception() -> None:
    """A scoped run should never remain running after an exception."""
    lake = FakeLake()
    tracker = PipelineRunTracker(lake)  # type: ignore[arg-type]

    with (
        pytest.raises(ValueError, match="bad provider response"),
        tracker.track_run(flow_name="flow", domain="domain", run_kind="daily"),
    ):
        raise ValueError("bad provider response")

    assert lake.inserted[0][1] == "runs"
    assert lake.executed[-1][1] is not None
    assert lake.executed[-1][1][-3] == "ValueError"
    assert lake.executed[-1][1][-2] == "bad provider response"


def test_track_run_allows_explicit_completion() -> None:
    """A scoped run should preserve an explicit successful terminal state."""
    lake = FakeLake()
    tracker = PipelineRunTracker(lake)  # type: ignore[arg-type]

    with tracker.track_run(flow_name="flow", domain="domain", run_kind="daily") as run:
        run.complete(counters=RunCounters(units_total=0), summary={"ok": True})

    assert lake.inserted[0][1] == "runs"
    assert lake.executed[-1][1] is not None
    assert lake.executed[-1][1][0] == "completed"
    assert lake.executed[-1][1][2] == 0
    assert lake.executed[-1][1][-2] == '{"ok":true}'


def test_track_unit_binds_run_context_and_landing_object() -> None:
    """Unit scopes should remove repeated run/domain/provider audit arguments."""
    lake = FakeLake()
    tracker = PipelineRunTracker(lake)  # type: ignore[arg-type]

    with tracker.track_run(
        flow_name="exchange-refresh", domain="exchange", run_kind="snapshot", provider="eodhd"
    ) as run:
        with run.track_unit(unit_type="catalog_snapshot", unit_key={"snapshot_date": "2026-05-22"}) as unit:
            landing = LandingWrite(
                dataset="exchange.catalog",
                source_uri="s3://bucket/exchange.jsonl",
                partition={"snapshot_date": "2026-05-22"},
                rows_raw=10,
            )
            unit.complete_with_landing(landing, rows_written=10)
        run.complete(rows_raw=10, rows_written=10)

    assert [item[1] for item in lake.inserted] == ["runs", "run_units", "landing_objects"]
    unit_row = lake.inserted[1][2][0]
    landing_row = lake.inserted[2][2][0]
    assert unit_row["domain"] == "exchange"
    assert unit_row["provider"] == "eodhd"
    assert unit_row["status"] == "completed"
    assert unit_row["source_uri"] == "s3://bucket/exchange.jsonl"
    assert unit_row["rows_raw"] == 10
    assert landing_row["unit_id"] == unit_row["unit_id"]
    assert lake.executed[-1][1] is not None
    assert lake.executed[-1][1][2] == 1
    assert lake.executed[-1][1][3] == 1


def test_track_unit_records_failure_on_exception() -> None:
    """A failing unit scope should create a failed unit before the run fails."""
    lake = FakeLake()
    tracker = PipelineRunTracker(lake)  # type: ignore[arg-type]

    with (
        pytest.raises(ValueError, match="fetch failed"),
        tracker.track_run(flow_name="exchange-refresh", domain="exchange", run_kind="snapshot") as run,
        run.track_unit(unit_type="catalog_snapshot", unit_key={"snapshot_date": "2026-05-22"}),
    ):
        raise ValueError("fetch failed")

    unit_row = lake.inserted[1][2][0]
    assert unit_row["status"] == "failed"
    assert unit_row["error_class"] == "ValueError"
    assert lake.executed[-1][1] is not None
    assert lake.executed[-1][1][1] == 1
    assert lake.executed[-1][1][3] == 1


def test_run_scope_records_unit_with_landing_object() -> None:
    """Run scopes should support loop-friendly unit + landing recording."""
    lake = FakeLake()
    tracker = PipelineRunTracker(lake)  # type: ignore[arg-type]
    landing = LandingWrite(
        dataset="instrument.catalog",
        source_uri="s3://bucket/instrument.jsonl",
        partition={"provider_exchange_code": "US", "snapshot_date": "2026-05-22"},
        rows_raw=100,
    )

    with tracker.track_run(flow_name="instrument-refresh", domain="instrument", run_kind="snapshot") as run:
        unit_id = run.record_unit_with_landing(
            landing,
            unit_type="exchange_snapshot",
            unit_key={"provider_exchange_code": "US", "snapshot_date": "2026-05-22"},
            rows_written=99,
        )
        run.complete(rows_raw=100, rows_written=99)

    assert [item[1] for item in lake.inserted] == ["runs", "run_units", "landing_objects"]
    assert lake.inserted[1][2][0]["unit_id"] == unit_id
    assert lake.inserted[1][2][0]["source_uri"] == landing.source_uri
    assert lake.inserted[2][2][0]["unit_id"] == unit_id
    assert lake.inserted[2][2][0]["dataset"] == "instrument.catalog"


def test_track_run_requires_terminal_state() -> None:
    """Exiting a scope without complete/fail should become a failed run and a test-visible error."""
    lake = FakeLake()
    tracker = PipelineRunTracker(lake)  # type: ignore[arg-type]

    with (
        pytest.raises(RuntimeError, match="terminal state"),
        tracker.track_run(flow_name="flow", domain="domain", run_kind="daily"),
    ):
        pass

    assert lake.executed[-1][1] is not None
    assert lake.executed[-1][1][-2] == "Run scope exited without a terminal state."


def test_tracker_batches_units_landing_objects_and_rejections() -> None:
    """High-volume audit paths should support batched lake writes."""
    lake = FakeLake()
    tracker = PipelineRunTracker(lake)  # type: ignore[arg-type]
    run_id = tracker.start_run(flow_name="flow", domain="eod_price", run_kind="backfill")

    unit_ids = tracker.record_units(
        [
            RunUnitRecord(
                run_id=run_id,
                domain="eod_price",
                provider="eodhd",
                unit_type="ticker_backfill",
                unit_key={"ticker": "AAPL.US"},
                status="completed",
            ),
            RunUnitRecord(
                run_id=run_id,
                domain="eod_price",
                provider="eodhd",
                unit_type="ticker_backfill",
                unit_key={"ticker": "MSFT.US"},
                status="failed",
                error=RuntimeError("fetch failed"),
            ),
        ]
    )
    tracker.record_landing_objects(
        [
            LandingObjectRecord(
                run_id=run_id,
                unit_id=unit_ids[0],
                domain="eod_price",
                provider="eodhd",
                dataset="eod_price.backfill",
                source_uri="s3://bucket/aapl.jsonl",
            )
        ]
    )
    inserted = tracker.record_rejections(
        [
            RejectionRecord(
                run_id=run_id,
                unit_id=unit_ids[0],
                domain="eod_price",
                raw_fragment={"ticker": "AAPL.US", "close": None},
                reason="parse_rejected",
            ),
            RejectionRecord(
                run_id=run_id,
                unit_id=unit_ids[0],
                domain="eod_price",
                raw_fragment={"ticker": "AAPL.US", "open": None},
                reason="parse_rejected",
            ),
        ],
        limit=1,
    )

    assert inserted == 1
    assert [item[1] for item in lake.inserted] == ["runs", "run_units", "landing_objects", "rejections"]
    assert len(lake.inserted[1][2]) == 2
    assert lake.inserted[1][2][1]["error_class"] == "RuntimeError"
    assert len(lake.inserted[3][2]) == 1


def test_rejection_hash_includes_entity_context() -> None:
    """Identical raw fragments for different entities should both be recordable."""
    lake = FakeLake()
    tracker = PipelineRunTracker(lake)  # type: ignore[arg-type]
    run_id = tracker.start_run(flow_name="flow", domain="eod_price", run_kind="backfill")
    raw_fragment = {"date": "2026-05-22", "close": None}

    tracker.record_rejections(
        [
            RejectionRecord(
                run_id=run_id,
                domain="eod_price",
                entity_key={"ticker": "AAPL.US"},
                raw_fragment=raw_fragment,
                reason="parse_rejected",
            ),
            RejectionRecord(
                run_id=run_id,
                domain="eod_price",
                entity_key={"ticker": "MSFT.US"},
                raw_fragment=raw_fragment,
                reason="parse_rejected",
            ),
        ]
    )

    rejection_rows = lake.inserted[1][2]
    assert len(rejection_rows) == 2
    assert rejection_rows[0]["raw_hash"] != rejection_rows[1]["raw_hash"]
