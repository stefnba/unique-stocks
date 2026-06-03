"""Run-unit overview page for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.filters import RunUnitFilters, render_run_unit_controls
from dashboard.formatting import format_int
from dashboard.loaders import load_run_units_overview_page
from dashboard.routing import overview_href
from dashboard.tables import frame, render_run_unit_overview_table, search_frame
from dashboard.views.common import load_or_show_error, render_cache_caption, render_page_header

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

    unit_frame = search_frame(
        unit_frame,
        query=filters["search"],
        columns=_RUN_UNIT_SEARCH_COLUMNS,
    )
    if unit_frame.empty:
        st.info("No run units match the search query.")
        return

    st.subheader(f"{format_int(len(unit_frame))} run units")
    render_run_unit_overview_table(unit_frame, key="run_units_overview")


def _load_run_units(filters: RunUnitFilters) -> dict[str, Any] | None:
    """Load filtered run-unit overview data."""
    page = load_or_show_error(
        lambda: load_run_units_overview_page(
            since_iso=filters["since"].isoformat(),
            domains=tuple(filters["domains"]),
            statuses=tuple(filters["statuses"]),
            run_id=filters["run_id"],
            recent_limit=int(filters["recent_limit"]),
        ),
        error_label="Run units",
    )
    if page is None:
        return None
    render_cache_caption(filters["since"])
    return page
