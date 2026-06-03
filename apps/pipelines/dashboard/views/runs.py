"""Run overview page for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.filters import RunFilters, render_run_controls
from dashboard.formatting import format_int
from dashboard.loaders import load_runs_page
from dashboard.routing import overview_href
from dashboard.tables import frame, render_run_table, search_frame
from dashboard.views.common import (
    lake_ready,
    load_or_show_error,
    render_cache_caption,
    render_page_header,
)

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

    runs_frame = search_frame(
        runs_frame,
        query=filters["search"],
        columns=_RUN_SEARCH_COLUMNS,
    )
    if runs_frame.empty:
        st.info("No runs match the search query.")
        return

    st.subheader(f"{format_int(len(runs_frame))} runs")
    render_run_table(runs_frame, key="runs_overview")


def _load_runs(filters: RunFilters) -> dict[str, Any] | None:
    """Load filtered run overview data."""
    page = load_or_show_error(
        lambda: load_runs_page(
            since_iso=filters["since"].isoformat(),
            domains=tuple(filters["domains"]),
            statuses=tuple(filters["statuses"]),
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
