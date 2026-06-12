"""Tests for dashboard filter UI helpers and page-level filter plumbing."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from pytest import MonkeyPatch

from dashboard.filters import RunFilters, RunUnitFilters, status_values_from_filter
from dashboard.tables import all_filter_label
from dashboard.views import run_units as run_units_view
from dashboard.views import runs as runs_view


def test_status_filter_normalization() -> None:
    """Status presets should become query filters only when a real status is selected."""
    assert status_values_from_filter(None) == ()
    assert status_values_from_filter("") == ()
    assert status_values_from_filter("All statuses") == ()
    assert status_values_from_filter("failed") == ("failed",)


def test_table_all_filter_labels_are_pluralized() -> None:
    """Table dropdown all-options should read naturally in the UI."""
    assert all_filter_label("Status") == "All statuses"
    assert all_filter_label("Dataset") == "All datasets"
    assert all_filter_label("Domains") == "All domains"


def test_runs_status_preset_reaches_loader(monkeypatch: MonkeyPatch) -> None:
    """Runs page status presets should narrow the lake query before table filtering."""
    captured: dict[str, Any] = {}

    def fake_load_runs_page(
        *,
        since_iso: str,
        domains: tuple[str, ...],
        statuses: tuple[str, ...],
        recent_limit: int,
    ) -> dict[str, Any]:
        captured.update(
            {
                "since_iso": since_iso,
                "domains": domains,
                "statuses": statuses,
                "recent_limit": recent_limit,
            }
        )
        return {"available": True, "recent_runs": []}

    monkeypatch.setattr(runs_view, "load_runs_page", fake_load_runs_page)
    monkeypatch.setattr(runs_view, "render_cache_caption", lambda since: None)
    monkeypatch.setattr(runs_view, "lake_ready", lambda page: True)

    since = datetime(2026, 6, 11, 10, 0, tzinfo=UTC)
    page = runs_view._load_runs(
        cast(
            RunFilters,
            {
                "since": since,
                "domains": ["eod_price"],
                "recent_limit": 1000,
                "status_filter": "failed",
            },
        )
    )

    assert page == {"available": True, "recent_runs": []}
    assert captured["since_iso"] == since.isoformat()
    assert captured["domains"] == ("eod_price",)
    assert captured["statuses"] == ("failed",)
    assert captured["recent_limit"] == 1000


def test_run_units_status_preset_reaches_loader(monkeypatch: MonkeyPatch) -> None:
    """Run-unit page status presets should narrow the lake query before table filtering."""
    captured: dict[str, Any] = {}

    def fake_load_run_units_overview_page(
        *,
        since_iso: str,
        domains: tuple[str, ...],
        statuses: tuple[str, ...],
        run_id: str | None,
        recent_limit: int,
    ) -> dict[str, Any]:
        captured.update(
            {
                "since_iso": since_iso,
                "domains": domains,
                "statuses": statuses,
                "run_id": run_id,
                "recent_limit": recent_limit,
            }
        )
        return {"available": True, "recent_units": []}

    monkeypatch.setattr(run_units_view, "load_run_units_overview_page", fake_load_run_units_overview_page)
    monkeypatch.setattr(run_units_view, "render_cache_caption", lambda since: None)

    since = datetime(2026, 6, 11, 10, 0, tzinfo=UTC)
    page = run_units_view._load_run_units(
        cast(
            RunUnitFilters,
            {
                "since": since,
                "domains": ["fundamental"],
                "recent_limit": 1000,
                "run_id": "run-1",
                "status_filter": "unsupported",
            },
        )
    )

    assert page == {"available": True, "recent_units": []}
    assert captured["since_iso"] == since.isoformat()
    assert captured["domains"] == ("fundamental",)
    assert captured["statuses"] == ("unsupported",)
    assert captured["run_id"] == "run-1"
    assert captured["recent_limit"] == 1000
