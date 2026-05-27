"""Tests for pipeline audit tracking helpers."""

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from core.ingestion.run_tracking import PipelineRunTracker, RunCounters, terminal_status


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
