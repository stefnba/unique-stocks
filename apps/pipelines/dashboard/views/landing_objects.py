"""Landing-object pages for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.filters import LandingObjectFilters, render_landing_object_controls
from dashboard.formatting import (
    format_datetime,
    format_int,
    json_value_parsed,
    jsonable,
    safe_key,
    short_id,
)
from dashboard.loaders import load_landing_object_page, load_landing_objects_overview_page
from dashboard.routing import landing_objects_href, overview_href, run_detail_href, unit_detail_href
from dashboard.tables import frame, render_landing_object_overview_table, search_frame
from dashboard.views.common import load_or_show_error, render_cache_caption, render_page_header
from dashboard.views.components import render_key_fields, render_run_status_callout, render_unit_status_callout

_LANDING_OBJECT_SEARCH_COLUMNS = [
    "landing_id",
    "run_id",
    "unit_id",
    "flow_name",
    "domain",
    "provider",
    "dataset",
    "source_uri",
    "partition_json",
    "content_hash",
]


def render_landing_objects_page() -> None:
    """Render the searchable landing-object overview route."""
    render_page_header(
        title="Landing Objects",
        caption="Search and filter raw landing-object evidence captured before parsing.",
        breadcrumb=(("Pipeline Audit", overview_href()), ("Landing Objects", None)),
    )
    filters = render_landing_object_controls()
    page = _load_landing_objects(filters)
    if page is None:
        return
    if not page["available"]:
        st.warning("pipeline.landing_objects is not available in the configured lake.")
        return

    landing_frame = frame(page["recent_landing_objects"])
    if landing_frame.empty:
        st.info("No landing objects match the selected filters.")
        return

    landing_frame = search_frame(
        landing_frame,
        query=filters["search"],
        columns=_LANDING_OBJECT_SEARCH_COLUMNS,
    )
    if landing_frame.empty:
        st.info("No landing objects match the search query.")
        return

    st.subheader(f"{format_int(len(landing_frame))} landing objects")
    render_landing_object_overview_table(landing_frame, key="landing_objects_overview")


def render_landing_object_detail_page(landing_id: str | None) -> None:
    """Render one landing object's drill-down route.

    Args:
        landing_id: Landing-object identifier from query params.
    """
    render_page_header(
        title="Landing Object Detail",
        caption="Raw capture metadata and parent run-unit context.",
        breadcrumb=(("Pipeline Audit", overview_href()), ("Landing Objects", landing_objects_href()), ("Object", None)),
    )
    if not landing_id:
        st.warning("Choose a landing object from the Landing Objects page to open its detail page.")
        return

    page = load_or_show_error(
        lambda: load_landing_object_page(landing_id),
        error_label="Landing object detail",
    )
    if page is None:
        return
    if not page["available"]:
        st.warning("pipeline.landing_objects is not available in the configured lake.")
        return

    landing_object = page["landing_object"]
    if landing_object is None:
        st.warning(f"Landing object {landing_id} was not found in pipeline.landing_objects.")
        return

    _render_landing_object_summary(landing_object)
    _render_parent_context(run=page["run"], unit=page["unit"], landing_object=landing_object)
    _render_landing_object_payload(landing_object)


def _load_landing_objects(filters: LandingObjectFilters) -> dict[str, Any] | None:
    """Load filtered landing-object overview data."""
    page = load_or_show_error(
        lambda: load_landing_objects_overview_page(
            since_iso=filters["since"].isoformat(),
            domains=tuple(filters["domains"]),
            run_id=filters["run_id"],
            unit_id=filters["unit_id"],
            recent_limit=int(filters["recent_limit"]),
        ),
        error_label="Landing objects",
    )
    if page is None:
        return None
    render_cache_caption(filters["since"])
    return page


def _render_landing_object_summary(landing_object: dict[str, Any]) -> None:
    """Render landing-object identity, scan fields, and metrics."""
    st.title(str(landing_object.get("dataset") or "Landing object"))
    st.caption(
        " · ".join(
            value
            for value in (
                str(landing_object.get("domain") or ""),
                str(landing_object.get("provider") or ""),
                f"recorded {format_datetime(landing_object.get('recorded_at'))}",
                f"landing {short_id(landing_object.get('landing_id'))}",
            )
            if value
        )
    )
    st.markdown(f"Landing ID `{landing_object['landing_id']}`")

    dataset, rows_raw, byte_count, content_hash = st.columns(4)
    dataset.metric("Dataset", str(landing_object.get("dataset") or "-"))
    rows_raw.metric("Rows Raw", format_int(landing_object.get("rows_raw")))
    byte_count.metric("Bytes", format_int(landing_object.get("byte_count")))
    content_hash.metric("Content Hash", short_id(landing_object.get("content_hash")))

    source_uri = landing_object.get("source_uri")
    if source_uri:
        st.markdown(f"Source URI `{source_uri}`")

    render_landing_object_overview_table(
        frame([landing_object]),
        key=f"landing_object_detail_{safe_key(str(landing_object.get('landing_id')))}",
    )


def _render_parent_context(
    *,
    run: dict[str, Any] | None,
    unit: dict[str, Any] | None,
    landing_object: dict[str, Any],
) -> None:
    """Render linked parent run and unit context."""
    st.subheader("Parent context")
    run_id = landing_object.get("run_id")
    unit_id = landing_object.get("unit_id")
    run_column, unit_column = st.columns(2)
    if run_id:
        run_column.link_button("Open Run", run_detail_href(run_id), icon=":material/history:", width="stretch")
    else:
        run_column.info("No parent run recorded.")
    if run_id and unit_id:
        unit_column.link_button(
            "Open Run Unit",
            unit_detail_href(run_id=str(run_id), unit_id=unit_id),
            icon=":material/view_list:",
            width="stretch",
        )
    else:
        unit_column.info("No parent unit recorded.")

    if run is not None:
        st.markdown(f"**Run:** {run.get('flow_name') or short_id(run.get('run_id'))}")
        render_run_status_callout(run)
    if unit is not None:
        st.markdown(f"**Run Unit:** {short_id(unit.get('unit_id'))}")
        render_unit_status_callout(unit)
        render_key_fields(unit.get("unit_key_json"))


def _render_landing_object_payload(landing_object: dict[str, Any]) -> None:
    """Render raw landing-object metadata and parsed partition JSON."""
    with st.expander("Partition", expanded=True):
        st.json(json_value_parsed(landing_object.get("partition_json")))
    with st.expander("Landing Metadata"):
        st.json(
            jsonable(
                {
                    "landing_id": landing_object.get("landing_id"),
                    "run_id": landing_object.get("run_id"),
                    "unit_id": landing_object.get("unit_id"),
                    "domain": landing_object.get("domain"),
                    "provider": landing_object.get("provider"),
                    "dataset": landing_object.get("dataset"),
                    "source_uri": landing_object.get("source_uri"),
                    "rows_raw": landing_object.get("rows_raw"),
                    "byte_count": landing_object.get("byte_count"),
                    "content_hash": landing_object.get("content_hash"),
                    "recorded_at": landing_object.get("recorded_at"),
                }
            )
        )
