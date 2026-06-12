"""Tests for pure dashboard presentation helpers."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from dashboard.domain_health import build_domain_health_rows
from dashboard.tables import (
    compact_run_frame,
    filter_frame_by_values,
    search_frame,
    unit_key_columns_frame,
)


def test_compact_run_frame_formats_links_and_duration() -> None:
    """Run table shaping should add detail links and compact durations."""
    source = pd.DataFrame(
        [
            {
                "run_id": "018f0000-0000-7000-8000-000000000001",
                "status": "failed",
                "domain": "eod_price",
                "flow_name": "eod-price-daily",
                "run_kind": "daily",
                "started_at": datetime(2026, 6, 11, 10, 0, tzinfo=UTC),
                "duration_seconds": 125,
                "error_message": "x" * 120,
            }
        ]
    )

    result = compact_run_frame(source)

    assert list(result.columns) == [
        "run_id_short",
        "status",
        "domain",
        "flow_name",
        "run_kind",
        "started_at",
        "duration",
        "error_message",
    ]
    assert str(result.loc[0, "run_id_short"]).startswith("run-detail?run_id=018f0000")
    assert result.loc[0, "duration"] == "2m 5s"
    assert str(result.loc[0, "started_at"]) == "2026-06-11 10:00 UTC"
    assert str(result.loc[0, "error_message"]).endswith("...")


def test_unit_key_columns_frame_expands_json_and_avoids_collisions() -> None:
    """Unit-key JSON should become stable table columns without clobbering raw columns."""
    source = pd.DataFrame(
        [
            {
                "provider_exchange_code": "existing",
                "unit_key_json": {"provider_exchange_code": "US", "bar_date": "2026-06-11"},
            }
        ]
    )

    result, columns = unit_key_columns_frame(source)

    assert columns == ["key_provider_exchange_code", "bar_date"]
    assert result.to_dict(orient="records") == [{"key_provider_exchange_code": "US", "bar_date": "2026-06-11"}]


def test_search_and_value_filters_return_expected_rows() -> None:
    """Table search and value filters should produce deterministic dataframe slices."""
    source = pd.DataFrame(
        [
            {"status": "failed", "message": "Provider timeout"},
            {"status": "completed", "message": "Loaded prices"},
        ]
    )

    failed = filter_frame_by_values(source, column="status", values=["failed"])
    matching = search_frame(source, query="timeout", columns=["message"])
    empty = filter_frame_by_values(source, column="status", values=[])

    assert failed.to_dict(orient="records") == [{"status": "failed", "message": "Provider timeout"}]
    assert matching.to_dict(orient="records") == [{"status": "failed", "message": "Provider timeout"}]
    assert empty.empty


def test_build_domain_health_rows_prioritizes_attention_domains() -> None:
    """Domain health rows should classify and sort attention domains first."""
    failed_at = datetime(2026, 6, 11, 10, 0, tzinfo=UTC)
    completed_at = datetime(2026, 6, 11, 9, 0, tzinfo=UTC)

    rows = build_domain_health_rows(
        latest_rows=[
            {
                "domain": "eod_price",
                "status": "failed",
                "started_at": failed_at,
                "run_id": "018f0000-0000-7000-8000-000000000001",
                "flow_name": "eod-price-daily",
            },
            {
                "domain": "instrument",
                "status": "completed",
                "started_at": completed_at,
                "completed_at": completed_at,
                "run_id": "018f0000-0000-7000-8000-000000000002",
                "flow_name": "instrument-refresh",
            },
        ],
        terminal_rows=[
            {
                "domain": "instrument",
                "completed_at": completed_at,
            }
        ],
        attention_rows=[
            {
                "domain": "eod_price",
                "run_id": "018f0000-0000-7000-8000-000000000001",
                "started_at": failed_at,
            }
        ],
        summary_rows=[
            {"domain": "eod_price", "runs": 1, "attention_runs": 1, "rows_rejected": 5},
            {"domain": "instrument", "runs": 1, "rows_written": 10},
        ],
    )

    assert [row["domain"] for row in rows] == ["eod_price", "instrument"]
    assert [row["health_status"] for row in rows] == ["attention", "healthy"]
    assert rows[0]["attention_runs"] == 1
    assert rows[1]["rows_written"] == 10
