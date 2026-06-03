"""Run detail page for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.formatting import (
    format_datetime,
    format_duration,
    format_int,
    format_target_window,
    format_unit_count,
    safe_key,
    short_id,
)
from dashboard.loaders import load_run_page
from dashboard.routing import (
    landing_objects_href,
    overview_href,
    run_detail_href,
    run_units_href,
    runs_href,
    unit_detail_href,
)
from dashboard.tables import (
    default_unit_statuses,
    filter_frame_by_values,
    frame,
    render_unit_table,
    search_frame,
)
from dashboard.views.common import render_breadcrumb_bar
from dashboard.views.components import (
    prefect_flow_run_url,
    render_dbt_table,
    render_landing_table,
    render_rejection_table,
    render_run_payload,
    render_run_status_callout,
    render_unit_status_summary,
    render_units_limit_note,
)


def render_run_page(run_id: str | None, *, preview_unit_id: str | None = None) -> None:
    """Render one run's drill-down route.

    Args:
        run_id: Durable run identifier from ``pipeline.runs``.
        preview_unit_id: Optional unit id from query params for inline evidence preview.
    """
    render_breadcrumb_bar(("Pipeline Audit", overview_href()), ("Runs", runs_href()), ("Run", None))

    if not run_id:
        st.title("Run Detail")
        st.caption("Parent run investigation and related work-unit evidence.")
        st.warning("Choose a run from the overview to open its detail page.")
        return

    try:
        page = load_run_page(run_id)
    except Exception as exc:
        st.error("Run detail is not reachable.")
        st.caption(f"{type(exc).__name__}: {exc}")
        return

    run = page["run"]
    if run is None:
        st.title("Run Detail")
        st.warning(f"Run {run_id} was not found in pipeline.runs.")
        return

    _render_run_summary(run)
    _render_run_detail(run, page["detail"], preview_unit_id=preview_unit_id)


def _render_run_summary(run: dict[str, Any]) -> None:
    """Render the run header, status callout, and summary metrics.

    Args:
        run: Run row from ``pipeline.runs``.
    """
    st.title(str(run["flow_name"]))
    st.caption(
        " · ".join(
            value
            for value in (
                str(run.get("domain") or ""),
                str(run.get("run_kind") or ""),
                str(run.get("provider") or ""),
                format_target_window(run),
                f"started {format_datetime(run.get('started_at'))}",
                f"run {short_id(run.get('run_id'))}",
            )
            if value
        )
    )
    if run.get("run_id"):
        st.markdown(f"Run ID `{run['run_id']}`")
    render_run_status_callout(run)

    left, middle, units, failed_units, written, rejected = st.columns(6)
    left.metric("Status", str(run["status"]))
    middle.metric("Duration", format_duration(run.get("duration_seconds")))
    units.metric("Units", format_unit_count(run))
    failed_units.metric("Failed Units", format_int(run.get("units_failed")))
    written.metric("Rows Written", format_int(run.get("rows_written")))
    rejected.metric("Rows Rejected", format_int(run.get("rows_rejected")))

    action_columns = st.columns(4)
    prefect_url = prefect_flow_run_url(run.get("prefect_flow_run_id"))
    if prefect_url:
        action_columns[0].link_button("Open in Prefect", prefect_url)
    if run.get("parent_run_id"):
        action_columns[1].link_button("Open parent run", run_detail_href(run["parent_run_id"]))
    action_columns[2].link_button(
        "Run Units",
        run_units_href(run_id=str(run["run_id"])),
        icon=":material/view_list:",
        width="stretch",
    )
    action_columns[3].link_button(
        "Landing Objects",
        landing_objects_href(run_id=str(run["run_id"])),
        icon=":material/cloud:",
        width="stretch",
    )


def _render_run_detail(
    run: dict[str, Any],
    detail: dict[str, list[dict[str, Any]]],
    *,
    preview_unit_id: str | None = None,
) -> None:
    """Render investigation tabs for one run.

    Args:
        run: Run row from ``pipeline.runs``.
        detail: Nested evidence lists loaded for the run.
        preview_unit_id: Optional unit id from query params for inline evidence preview.
    """
    st.subheader("Investigation")
    units, rejections, landing, dbt, payload = st.tabs(
        [
            f"Work Units ({len(detail['run_units'])})",
            f"Rejections ({len(detail['rejections'])})",
            f"Landing Objects ({len(detail['landing_objects'])})",
            f"dbt Nodes ({len(detail['dbt_node_results'])})",
            "Run Payload",
        ]
    )
    with units:
        _render_units_panel(
            unit_status_rows=detail["unit_status"],
            unit_rows=detail["run_units"],
            landing_rows=detail["landing_objects"],
            rejection_rows=detail["rejections"],
            run_id=str(run["run_id"]),
            preview_unit_id=preview_unit_id,
        )

    with rejections:
        render_rejection_table(detail["rejections"])

    with landing:
        render_landing_table(detail["landing_objects"], key=f"run_landing_{run['run_id']}")

    with dbt:
        render_dbt_table(detail["dbt_node_results"])

    with payload:
        render_run_payload(run)


def _render_units_panel(
    *,
    unit_status_rows: list[dict[str, Any]],
    unit_rows: list[dict[str, Any]],
    landing_rows: list[dict[str, Any]],
    rejection_rows: list[dict[str, Any]],
    run_id: str,
    preview_unit_id: str | None = None,
) -> None:
    """Render the filterable unit table and selected-unit evidence panel.

    Args:
        unit_status_rows: Unit status breakdown rows for summary metrics.
        unit_rows: Work-unit rows for the run.
        landing_rows: Run-scoped landing-object rows used for inline evidence.
        rejection_rows: Run-scoped rejection rows used for inline evidence.
        run_id: Parent run identifier used for widget keys and unit links.
        preview_unit_id: Optional unit id from query params for inline evidence preview.
    """
    render_unit_status_summary(unit_status_rows)

    unit_frame = frame(unit_rows)
    if unit_frame.empty:
        st.info("No work units recorded for this run. dbt runs use the dbt node tab instead.")
        return

    status_options = [str(value) for value in unit_frame["status"].dropna().unique().tolist()]
    selected_statuses = [
        str(value)
        for value in st.multiselect(
            "Unit statuses",
            options=status_options,
            default=default_unit_statuses(unit_frame),
            key=f"unit_status_filter_{run_id}",
        )
    ]
    search = str(
        st.text_input(
            "Find unit",
            placeholder="Search key, reason, error, hash, or source URI",
            key=f"unit_search_{run_id}",
        )
    ).strip()
    filtered = filter_frame_by_values(unit_frame, column="status", values=selected_statuses)
    filtered = search_frame(
        filtered,
        query=search,
        columns=["unit_key_json", "unit_key_hash", "reason", "source_uri", "error_class", "error_message"],
    )

    if filtered.empty:
        st.info("No work units match the selected unit filters.")
        return

    selected_unit_id = render_unit_table(filtered, run_id=run_id, key=f"run_units_{safe_key(run_id)}")
    render_units_limit_note(unit_rows)

    active_unit_id = selected_unit_id or preview_unit_id
    if active_unit_id:
        st.markdown("**Unit evidence**")
        st.link_button("Open full unit detail", unit_detail_href(run_id=run_id, unit_id=active_unit_id))
        unit_landing = [row for row in landing_rows if str(row.get("unit_id")) == active_unit_id]
        unit_rejections = [row for row in rejection_rows if str(row.get("unit_id")) == active_unit_id]
        evidence_landing, evidence_rejections = st.tabs(
            [
                f"Landing ({len(unit_landing)})",
                f"Rejections ({len(unit_rejections)})",
            ]
        )
        with evidence_landing:
            render_landing_table(unit_landing, key=f"run_unit_preview_landing_{safe_key(active_unit_id)}")
        with evidence_rejections:
            render_rejection_table(unit_rejections)
