"""Run overview page for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.filters import RunFilters, render_run_controls, status_values_from_filter
from dashboard.formatting import format_int
from dashboard.loaders import load_runs_page
from dashboard.routing import overview_href
from dashboard.tables import frame, render_run_table, table_browser_frame
from dashboard.views.common import (
    lake_ready,
    load_or_show_error,
    render_cache_caption,
    render_page_header,
)
from dashboard.views.components import render_browse_limit_note

_RUN_SEARCH_COLUMNS = [
    "run_id",
    "parent_run_id",
    "prefect_flow_run_id",
    "flow_name",
    "domain",
    "provider",
    "status",
    "error_class",
    "error_message",
    "run_kind",
]


def render_runs_page() -> None:
    """Render the searchable run overview route."""
    render_page_header(
        title="Runs",
        caption="Search and filter the pipeline run audit trail.",
        breadcrumb=(("Pipeline Audit", overview_href()), ("Runs", None)),
    )
    filters = render_run_controls()
    page = _load_runs(filters)
    if page is None:
        return

    runs_frame = frame(page["recent_runs"])
    if runs_frame.empty:
        st.info("No runs match the selected filters.")
        return

    st.subheader(f"{format_int(len(runs_frame))} runs")
    render_browse_limit_note(page["recent_runs"], limit=filters["recent_limit"], label="runs")
    visible_runs = table_browser_frame(
        runs_frame,
        key="runs_overview",
        label="runs",
        filter_column="status",
        filter_label="Status",
        filter_default=filters["status_filter"],
        search_columns=_RUN_SEARCH_COLUMNS,
        search_placeholder="Run id, flow, domain, provider, error, or message",
    )
    if visible_runs.empty:
        return
    render_run_table(visible_runs, key="runs_overview")


def _load_runs(filters: RunFilters) -> dict[str, Any] | None:
    """Load filtered run overview data."""
    page = load_or_show_error(
        lambda: load_runs_page(
            since_iso=filters["since"].isoformat(),
            domains=tuple(filters["domains"]),
            statuses=status_values_from_filter(filters["status_filter"]),
            recent_limit=int(filters["recent_limit"]),
        ),
        error_label="Runs",
    )
    if page is None:
        return None
    render_cache_caption(filters["since"])
    if not lake_ready(page):
        return None
    return page
