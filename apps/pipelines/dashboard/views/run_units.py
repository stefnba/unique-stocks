"""Run-unit overview page for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.filters import RunUnitFilters, render_run_unit_controls, status_values_from_filter
from dashboard.formatting import format_int
from dashboard.loaders import load_run_units_overview_page
from dashboard.routing import overview_href
from dashboard.tables import frame, render_run_unit_overview_table, table_browser_frame
from dashboard.views.common import load_or_show_error, render_cache_caption, render_page_header
from dashboard.views.components import render_browse_limit_note

_RUN_UNIT_SEARCH_COLUMNS = [
    "unit_id",
    "run_id",
    "flow_name",
    "domain",
    "provider",
    "unit_type",
    "unit_key_hash",
    "unit_key_json",
    "status",
    "reason",
    "source_uri",
    "error_class",
    "error_message",
]


def render_run_units_page() -> None:
    """Render the searchable run-unit overview route."""
    render_page_header(
        title="Run Units",
        caption="Search and filter work-unit level audit records across runs.",
        breadcrumb=(("Pipeline Audit", overview_href()), ("Run Units", None)),
    )
    filters = render_run_unit_controls()
    page = _load_run_units(filters)
    if page is None:
        return
    if not page["available"]:
        st.warning("pipeline.run_units is not available in the configured lake.")
        return

    unit_frame = frame(page["recent_units"])
    if unit_frame.empty:
        st.info("No run units match the selected filters.")
        return

    st.subheader(f"{format_int(len(unit_frame))} run units")
    render_browse_limit_note(page["recent_units"], limit=filters["recent_limit"], label="run units")
    visible_units = table_browser_frame(
        unit_frame,
        key="run_units_overview",
        label="run units",
        filter_column="status",
        filter_label="Status",
        filter_default=filters["status_filter"],
        search_columns=_RUN_UNIT_SEARCH_COLUMNS,
        search_placeholder="Unit id, key, hash, reason, source URI, or error",
    )
    if visible_units.empty:
        return
    render_run_unit_overview_table(
        visible_units,
        key="run_units_overview",
    )


def _load_run_units(filters: RunUnitFilters) -> dict[str, Any] | None:
    """Load filtered run-unit overview data."""
    page = load_or_show_error(
        lambda: load_run_units_overview_page(
            since_iso=filters["since"].isoformat(),
            domains=tuple(filters["domains"]),
            statuses=status_values_from_filter(filters["status_filter"]),
            run_id=filters["run_id"],
            recent_limit=int(filters["recent_limit"]),
        ),
        error_label="Run units",
    )
    if page is None:
        return None
    render_cache_caption(filters["since"])
    return page
