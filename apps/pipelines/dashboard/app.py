"""Streamlit entrypoint for monitoring pipeline audit runs."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict, cast
from urllib.parse import quote

import pandas as pd
import streamlit as st
from pandas.io.formats.style import Styler

from core.clients.lake import DataLakeClient
from dashboard.queries import (
    DEFAULT_DASHBOARD_DOMAINS,
    RUN_STATUSES,
    load_daily_run_trend,
    load_dbt_node_results,
    load_landing_objects,
    load_latest_runs_by_domain,
    load_latest_terminal_runs_by_domain,
    load_recent_runs,
    load_rejections,
    load_run_by_id,
    load_run_unit_by_id,
    load_run_units,
    load_stale_running_runs,
    load_status_breakdown,
    load_status_summary,
    load_unit_status_breakdown,
    pipeline_runs_available,
)

_CACHE_TTL_SECONDS = 60
_RUN_UNITS_LIMIT = 500
_LANDING_OBJECTS_LIMIT = 200
_REJECTIONS_LIMIT = 100
_DBT_NODE_RESULTS_LIMIT = 300
_OVERVIEW_PAGE = "overview"
_RUN_PAGE = "run"
_UNIT_PAGE = "unit"
_ATTENTION_STATUSES = {"failed", "partial"}
_UNIT_ATTENTION_STATUSES = {"failed", "unsupported", "skipped"}
_HEALTHY_RUN_STATUSES = {"completed", "partial", "skipped"}
_INTEGER_TABLE_COLUMNS = {
    "byte_count",
    "failures",
    "rows_affected",
    "rows_raw",
    "rows_rejected",
    "rows_valid",
    "rows_written",
    "units_failed",
    "units_succeeded",
    "units_total",
}
_FLOAT_TABLE_COLUMNS = {"execution_time"}


class SidebarFilters(TypedDict):
    """Typed values collected from dashboard controls."""

    since: datetime
    stale_after: datetime
    domains: list[str]
    statuses: list[str]
    recent_limit: int


def main() -> None:
    """Render the Streamlit dashboard."""
    _prefer_dashboard_motherduck_token()
    st.set_page_config(
        page_title="Pipeline Runs",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    route = _current_route()
    _render_navigation(route)

    if st.sidebar.button("Refresh", width="stretch"):
        st.cache_data.clear()

    if route["page"] == _RUN_PAGE:
        _render_run_page(route.get("run_id"))
        return

    if route["page"] == _UNIT_PAGE:
        _render_unit_page(route.get("run_id"), route.get("unit_id"))
        return

    _render_overview_page()


def _render_overview_page() -> None:
    """Render the run monitoring overview route."""
    st.title("Pipeline Audit")
    st.caption("Monitor data-plane run health, then drill into the exact units and evidence for any suspect run.")
    filters = _render_sidebar()

    try:
        snapshot = _load_snapshot(
            since_iso=filters["since"].isoformat(),
            stale_after_iso=filters["stale_after"].isoformat(),
            domains=tuple(filters["domains"]),
            statuses=tuple(filters["statuses"]),
            recent_limit=int(filters["recent_limit"]),
        )
    except Exception as exc:
        st.error("Lake audit data is not reachable.")
        st.caption(f"{type(exc).__name__}: {exc}")
        return

    st.caption(f"Window starts {_format_datetime(filters['since'])}. Cached for {_CACHE_TTL_SECONDS} seconds.")

    if not snapshot["available"]:
        st.warning("pipeline.runs is not available in the configured lake.")
        return

    _render_kpis(snapshot["summary"], snapshot["stale_runs"])
    _render_triage(snapshot)
    _render_domain_status(snapshot)
    _render_activity(snapshot)
    _render_recent_runs(snapshot["recent_runs"])


def _render_run_page(run_id: str | None) -> None:
    """Render one run's drill-down route."""
    st.caption("Pipeline Audit / Run")

    if st.button("Back to Overview"):
        _set_route(_OVERVIEW_PAGE)
        st.rerun()

    if not run_id:
        st.warning("Choose a run from the overview to open its detail page.")
        return

    try:
        page = _load_run_page(run_id)
    except Exception as exc:
        st.error("Run detail is not reachable.")
        st.caption(f"{type(exc).__name__}: {exc}")
        return

    run = page["run"]
    if run is None:
        st.warning(f"Run {run_id} was not found in pipeline.runs.")
        return

    _render_run_summary(run)
    _render_run_detail(run, page["detail"])


def _render_unit_page(run_id: str | None, unit_id: str | None) -> None:
    """Render one work unit's drill-down route."""
    st.caption("Pipeline Audit / Run / Unit")

    if run_id and st.button("Back to Run"):
        _set_route(_RUN_PAGE, run_id=run_id)
        st.rerun()

    if not run_id or not unit_id:
        st.warning("Choose a work unit from a run detail page to open its detail page.")
        return

    try:
        page = _load_unit_page(run_id=run_id, unit_id=unit_id)
    except Exception as exc:
        st.error("Unit detail is not reachable.")
        st.caption(f"{type(exc).__name__}: {exc}")
        return

    run = page["run"]
    unit = page["unit"]
    if run is None:
        st.warning(f"Run {run_id} was not found in pipeline.runs.")
        return
    if unit is None:
        st.warning(f"Unit {unit_id} was not found in pipeline.run_units for this run.")
        return

    _render_unit_summary(run, unit)
    _render_unit_detail(page["detail"])


def _render_sidebar() -> SidebarFilters:
    now = datetime.now(UTC)
    window_hours = int(
        cast(
            int,
            st.sidebar.selectbox(
                "Window",
                options=[24, 72, 168, 336, 720],
                index=2,
                format_func=_format_window,
            ),
        )
    )
    domains = [
        str(value)
        for value in st.sidebar.multiselect(
            "Domains",
            options=list(DEFAULT_DASHBOARD_DOMAINS),
            default=list(DEFAULT_DASHBOARD_DOMAINS),
        )
    ]
    statuses = [
        str(value)
        for value in st.sidebar.multiselect(
            "Statuses",
            options=list(RUN_STATUSES),
            default=list(RUN_STATUSES),
        )
    ]
    stale_hours = int(st.sidebar.number_input("Stale running hours", min_value=1, max_value=72, value=2, step=1))
    recent_limit = int(st.sidebar.slider("Recent rows", min_value=25, max_value=500, value=150, step=25))

    return {
        "since": now - timedelta(hours=window_hours),
        "stale_after": now - timedelta(hours=stale_hours),
        "domains": domains,
        "statuses": statuses,
        "recent_limit": recent_limit,
    }


@st.cache_data(ttl=_CACHE_TTL_SECONDS, show_spinner=False)
def _load_snapshot(
    *,
    since_iso: str,
    stale_after_iso: str,
    domains: tuple[str, ...],
    statuses: tuple[str, ...],
    recent_limit: int,
) -> dict[str, Any]:
    since = datetime.fromisoformat(since_iso)
    stale_after = datetime.fromisoformat(stale_after_iso)
    lake = DataLakeClient(read_only=True)
    try:
        available = pipeline_runs_available(lake)
        return {
            "available": available,
            "summary": load_status_summary(lake, since=since, domains=domains),
            "stale_runs": load_stale_running_runs(lake, older_than=stale_after, domains=domains),
            "latest_runs": load_latest_runs_by_domain(lake, domains=domains),
            "latest_terminal_runs": load_latest_terminal_runs_by_domain(lake, domains=domains, since=since),
            "status_breakdown": load_status_breakdown(lake, since=since, domains=domains),
            "daily_trend": load_daily_run_trend(lake, since=since, domains=domains),
            "recent_runs": load_recent_runs(
                lake,
                since=since,
                domains=domains,
                statuses=statuses,
                limit=recent_limit,
            ),
        }
    finally:
        lake.close()


@st.cache_data(ttl=_CACHE_TTL_SECONDS, show_spinner=False)
def _load_run_page(run_id: str) -> dict[str, Any]:
    lake = DataLakeClient(read_only=True)
    try:
        return {
            "run": load_run_by_id(lake, run_id=run_id),
            "detail": {
                "unit_status": load_unit_status_breakdown(lake, run_id=run_id),
                "run_units": load_run_units(lake, run_id=run_id, limit=_RUN_UNITS_LIMIT),
                "landing_objects": load_landing_objects(lake, run_id=run_id, limit=_LANDING_OBJECTS_LIMIT),
                "rejections": load_rejections(lake, run_id=run_id, limit=_REJECTIONS_LIMIT),
                "dbt_node_results": load_dbt_node_results(lake, run_id=run_id, limit=_DBT_NODE_RESULTS_LIMIT),
            },
        }
    finally:
        lake.close()


@st.cache_data(ttl=_CACHE_TTL_SECONDS, show_spinner=False)
def _load_unit_page(*, run_id: str, unit_id: str) -> dict[str, Any]:
    lake = DataLakeClient(read_only=True)
    try:
        return {
            "run": load_run_by_id(lake, run_id=run_id),
            "unit": load_run_unit_by_id(lake, run_id=run_id, unit_id=unit_id),
            "detail": {
                "landing_objects": load_landing_objects(
                    lake,
                    run_id=run_id,
                    unit_id=unit_id,
                    limit=_LANDING_OBJECTS_LIMIT,
                ),
                "rejections": load_rejections(
                    lake,
                    run_id=run_id,
                    unit_id=unit_id,
                    limit=_REJECTIONS_LIMIT,
                ),
            },
        }
    finally:
        lake.close()


def _render_kpis(summary: dict[str, int], stale_runs: list[dict[str, Any]]) -> None:
    columns = st.columns(6)
    columns[0].metric("Runs", _format_int(summary["total_runs"]))
    columns[1].metric("Attention", _format_int(summary["attention_runs"]))
    columns[2].metric("Running", _format_int(summary["running_runs"]))
    columns[3].metric("Stale", _format_int(len(stale_runs)))
    columns[4].metric("Rows Written", _format_int(summary["rows_written"]))
    columns[5].metric("Rows Rejected", _format_int(summary["rows_rejected"]))


def _render_triage(snapshot: dict[str, Any]) -> None:
    """Render the action queue for failed, partial, and stale runs."""
    st.subheader("Triage")
    st.caption("Start here when something needs investigation.")
    stale_runs = snapshot["stale_runs"]
    summary = snapshot["summary"]

    attention_runs = _attention_runs_frame(snapshot["recent_runs"])
    if not stale_runs and attention_runs.empty:
        st.success("No failed, partial, or stale runs in the selected window.")
        return

    if stale_runs:
        st.error(f"{len(stale_runs)} run(s) are stale in running state.")
        _render_run_table(_frame(stale_runs), key="stale_runs")

    if not attention_runs.empty:
        st.warning(f"{summary['attention_runs']} failed or partial run(s) in the selected window.")
        _render_run_table(attention_runs, key="attention_runs")


def _render_domain_status(snapshot: dict[str, Any]) -> None:
    """Render latest attempt and last healthy terminal run as domain cards."""
    st.subheader("Domain Status")
    domain_rows = _domain_health_rows(
        latest_rows=snapshot["latest_runs"],
        terminal_rows=snapshot["latest_terminal_runs"],
    )
    if not domain_rows:
        st.info("No runs found for the selected domains.")
        return

    for chunk in _chunks(domain_rows, 3):
        columns = st.columns(3)
        for column, row in zip(columns, chunk, strict=False):
            with column.container(border=True):
                st.markdown(f"**{row['domain']}**")
                st.caption(f"Last attempt: {row['last_attempt_status']}")
                if row["last_good_age"]:
                    st.caption("Last Good Run")
                    st.markdown(f"**{row['last_good_time']}**")
                    st.caption(row["last_good_age"])
                else:
                    st.caption("Last Good Run")
                    st.warning(row["last_good_time"])
                st.caption(f"Attempted {row['last_attempt_time']} · {row['last_attempt_flow']}")
                if row["last_attempt_run_id"]:
                    st.link_button("Open latest run", _run_detail_href(row["last_attempt_run_id"]), width="stretch")


def _render_activity(snapshot: dict[str, Any]) -> None:
    """Render chart-led activity context for the selected window."""
    st.subheader("Activity")
    trend, status = st.columns(2)
    with trend:
        st.markdown("**Daily runs**")
        trend_frame = _frame(snapshot["daily_trend"])
        if trend_frame.empty:
            st.info("No trend data in the selected window.")
        else:
            st.line_chart(trend_frame, x="run_date", y="runs", color="domain")

    with status:
        st.markdown("**Status mix**")
        status_frame = _frame(snapshot["status_breakdown"])
        if status_frame.empty:
            st.info("No status data in the selected window.")
        else:
            st.bar_chart(status_frame, x="status", y="runs", color="status")


def _render_recent_runs(rows: list[dict[str, Any]]) -> None:
    st.subheader("Run Lookup")
    if not rows:
        st.info("No recent runs match the selected filters.")
        return

    run_frame = _frame(rows)
    _render_run_table(run_frame, key="run_lookup")
    st.caption("Open any run to inspect work units, landing objects, rejections, dbt nodes, and raw payloads.")


def _render_run_summary(run: dict[str, Any]) -> None:
    st.title(str(run["flow_name"]))
    st.caption(
        " · ".join(
            value
            for value in (
                str(run.get("domain") or ""),
                str(run.get("run_kind") or ""),
                str(run.get("provider") or ""),
                _format_target_window(run),
                f"started {_format_datetime(run.get('started_at'))}",
                f"run {_short_id(run.get('run_id'))}",
            )
            if value
        )
    )
    if run.get("run_id"):
        st.markdown(f"Run ID `{run['run_id']}`")
    _render_run_status_callout(run)

    left, middle, units, failed_units, written, rejected = st.columns(6)
    left.metric("Status", str(run["status"]))
    middle.metric("Duration", _format_duration(run.get("duration_seconds")))
    units.metric("Units", _format_unit_count(run))
    failed_units.metric("Failed Units", _format_int(run.get("units_failed")))
    written.metric("Rows Written", _format_int(run.get("rows_written")))
    rejected.metric("Rows Rejected", _format_int(run.get("rows_rejected")))

    prefect_url = _prefect_flow_run_url(run.get("prefect_flow_run_id"))
    if prefect_url:
        st.link_button("Open in Prefect", prefect_url)
    if run.get("parent_run_id"):
        st.link_button("Open parent run", _run_detail_href(run["parent_run_id"]))


def _render_run_detail(run: dict[str, Any], detail: dict[str, list[dict[str, Any]]]) -> None:
    """Render investigation tabs for one run."""
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
            run_id=str(run["run_id"]),
        )

    with rejections:
        _render_rejection_table(detail["rejections"])

    with landing:
        _render_landing_table(detail["landing_objects"])

    with dbt:
        _render_dbt_table(detail["dbt_node_results"])

    with payload:
        _render_run_payload(run)


def _render_unit_summary(run: dict[str, Any], unit: dict[str, Any]) -> None:
    """Render header context for one work unit."""
    st.title(f"{unit.get('unit_type') or 'Work'} Unit")
    st.caption(
        " · ".join(
            value
            for value in (
                str(unit.get("domain") or ""),
                str(unit.get("provider") or ""),
                f"parent {run.get('flow_name')}",
                f"unit {_short_id(unit.get('unit_id'))}",
            )
            if value
        )
    )
    if unit.get("unit_id"):
        st.markdown(f"Unit ID `{unit['unit_id']}`")
    _render_unit_status_callout(unit)

    status, duration, raw, valid, written, rejected = st.columns(6)
    status.metric("Status", str(unit.get("status") or "-"))
    duration.metric("Duration", _format_duration(unit.get("duration_seconds")))
    raw.metric("Rows Raw", _format_int(unit.get("rows_raw")))
    valid.metric("Rows Valid", _format_int(unit.get("rows_valid")))
    written.metric("Rows Written", _format_int(unit.get("rows_written")))
    rejected.metric("Rows Rejected", _format_int(unit.get("rows_rejected")))

    reason = unit.get("reason")
    if reason:
        st.info(f"Reason: {reason}")

    _render_key_fields(unit.get("unit_key_json"))

    with st.expander("Raw Unit Key"):
        st.json(_json_value(unit.get("unit_key_json")))

    with st.expander("Unit Metadata"):
        st.json(
            _jsonable(
                {
                    "unit_id": unit.get("unit_id"),
                    "run_id": unit.get("run_id"),
                    "unit_key_hash": unit.get("unit_key_hash"),
                    "source_uri": unit.get("source_uri"),
                    "started_at": unit.get("started_at"),
                    "completed_at": unit.get("completed_at"),
                    "error_class": unit.get("error_class"),
                    "error_message": unit.get("error_message"),
                }
            )
        )


def _render_unit_detail(detail: dict[str, list[dict[str, Any]]]) -> None:
    """Render evidence tabs for one work unit."""
    st.subheader("Unit Evidence")
    landing, rejections = st.tabs(
        [
            f"Landing Objects ({len(detail['landing_objects'])})",
            f"Rejections ({len(detail['rejections'])})",
        ]
    )
    with landing:
        _render_landing_table(detail["landing_objects"])
    with rejections:
        _render_rejection_table(detail["rejections"])


def _render_units_panel(
    *,
    unit_status_rows: list[dict[str, Any]],
    unit_rows: list[dict[str, Any]],
    run_id: str,
) -> None:
    """Render filterable work-unit table and unit status chart."""
    status_frame = _frame(unit_status_rows)
    if not status_frame.empty:
        _render_unit_status_summary(status_frame)

    unit_frame = _frame(unit_rows)
    if unit_frame.empty:
        st.info("No work units recorded for this run. dbt runs use the dbt node tab instead.")
        return

    status_options = [str(value) for value in unit_frame["status"].dropna().unique().tolist()]
    default_statuses = [status for status in status_options if status in _UNIT_ATTENTION_STATUSES] or status_options
    selected_statuses = [
        str(value)
        for value in st.multiselect(
            "Unit statuses",
            options=status_options,
            default=default_statuses,
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
    filtered = _filter_frame_by_values(unit_frame, column="status", values=selected_statuses)
    filtered = _search_frame(
        filtered,
        query=search,
        columns=["unit_key_json", "unit_key_hash", "reason", "source_uri", "error_class", "error_message"],
    )

    if filtered.empty:
        st.info("No work units match the selected unit filters.")
        return

    _render_unit_table(filtered, run_id=run_id, key=f"run_units_{_safe_key(run_id)}")
    _render_limit_note(unit_rows, limit=_RUN_UNITS_LIMIT, label="work units")
    st.caption("Open a unit to inspect its key, landing objects, and rejection samples in one place.")


def _render_unit_table(frame: pd.DataFrame, *, run_id: str, key: str) -> None:
    """Render compact run-unit rows with unit drilldown links."""
    table = _compact_unit_frame(frame, run_id=run_id)
    state = st.dataframe(
        _styled_table(table),
        width="stretch",
        hide_index=True,
        key=key,
        on_select="rerun",
        selection_mode="single-row",
        column_config=_table_column_config(
            table,
            extra={
                "unit_url": st.column_config.LinkColumn("Unit", display_text="Open"),
            },
        ),
    )
    selected_index = _selected_row_index(state)
    unit_ids = frame["unit_id"].tolist() if "unit_id" in frame.columns else []
    selected_unit_id = _value_at(unit_ids, selected_index)
    if st.button("View selected unit", key=f"{key}_view_unit", disabled=selected_unit_id is None):
        _set_route(_UNIT_PAGE, run_id=run_id, unit_id=str(selected_unit_id))
        st.rerun()


def _render_unit_status_summary(frame: pd.DataFrame) -> None:
    """Render compact unit status counters."""
    columns = st.columns(min(4, max(1, len(frame))))
    for column, row in zip(columns, frame.to_dict(orient="records"), strict=False):
        with column:
            st.metric(_format_status(row.get("status")), _format_int(row.get("units")))


def _render_landing_table(rows: list[dict[str, Any]]) -> None:
    """Render landing objects as compact evidence rows."""
    frame = _frame(rows)
    if frame.empty:
        st.info("No landing objects recorded for this scope.")
        return
    columns = [
        "recorded_at",
        "dataset",
        "provider",
        "source_uri",
        "rows_raw",
        "byte_count",
        "content_hash",
        "partition_json",
        "unit_id",
    ]
    _render_compact_dataframe(frame, columns=columns)
    _render_limit_note(rows, limit=_LANDING_OBJECTS_LIMIT, label="landing objects")


def _render_rejection_table(rows: list[dict[str, Any]]) -> None:
    """Render sampled parser rejection rows."""
    frame = _frame(rows)
    if frame.empty:
        st.info("No sampled rejections recorded for this scope.")
        return
    columns = [
        "recorded_at",
        "reason",
        "error_class",
        "error_message",
        "entity_key_json",
        "source_uri",
        "raw_sample_json",
        "unit_id",
    ]
    _render_compact_dataframe(frame, columns=columns)
    _render_limit_note(rows, limit=_REJECTIONS_LIMIT, label="rejection samples")


def _render_dbt_table(rows: list[dict[str, Any]]) -> None:
    """Render dbt node result rows."""
    frame = _frame(rows)
    if frame.empty:
        st.info("No dbt node results recorded for this run.")
        return
    columns = [
        "status",
        "resource_type",
        "unique_id",
        "execution_time",
        "failures",
        "rows_affected",
        "relation_name",
        "message",
        "command",
        "target",
    ]
    _render_compact_dataframe(frame, columns=columns)
    _render_limit_note(rows, limit=_DBT_NODE_RESULTS_LIMIT, label="dbt node results")


def _render_run_payload(run: dict[str, Any]) -> None:
    """Render durable run metadata and JSON payloads."""
    run_metadata = {
        "run_id": run.get("run_id"),
        "parent_run_id": run.get("parent_run_id"),
        "prefect_flow_run_id": run.get("prefect_flow_run_id"),
        "domain": run.get("domain"),
        "run_kind": run.get("run_kind"),
        "provider": run.get("provider"),
        "environment": run.get("environment"),
        "code_version": run.get("code_version"),
        "target_window_start": run.get("target_window_start"),
        "target_window_end": run.get("target_window_end"),
        "started_at": run.get("started_at"),
        "completed_at": run.get("completed_at"),
        "error_class": run.get("error_class"),
        "error_message": run.get("error_message"),
    }
    with st.expander("Run Metadata", expanded=True):
        st.json(_jsonable(run_metadata))
    with st.expander("Parameters"):
        st.json(_json_value(run.get("parameters_json")))
    with st.expander("Summary"):
        st.json(_json_value(run.get("summary_json")))


def _render_run_table(frame: pd.DataFrame, *, key: str) -> None:
    table = _compact_run_frame(frame, include_links=True)
    state = st.dataframe(
        _styled_table(table),
        width="stretch",
        hide_index=True,
        key=key,
        on_select="rerun",
        selection_mode="single-row",
        column_config=_table_column_config(
            table,
            extra={
                "detail_url": st.column_config.LinkColumn("Run", display_text="Open"),
            },
        ),
    )
    selected_index = _selected_row_index(state)
    run_ids = frame["run_id"].tolist() if "run_id" in frame.columns else []
    selected_run_id = _value_at(run_ids, selected_index)
    if st.button("View selected run", key=f"{key}_view_run", disabled=selected_run_id is None):
        _set_route(_RUN_PAGE, run_id=str(selected_run_id))
        st.rerun()


def _render_compact_dataframe(frame: pd.DataFrame, *, columns: list[str]) -> None:
    """Render only available columns in a dataframe."""
    available_columns = [column for column in columns if column in frame.columns]
    table = cast(pd.DataFrame, frame.loc[:, available_columns].copy())
    for column in ("recorded_at", "started_at", "completed_at"):
        if column in table.columns:
            table[column] = [_format_datetime(value) for value in table[column].tolist()]
    for column in ("partition_json", "entity_key_json", "raw_sample_json"):
        if column in table.columns:
            table[column] = [_format_json_compact(value) for value in table[column].tolist()]
    for column in ("run_id", "unit_id", "landing_id", "rejection_id"):
        if column in table.columns:
            table[column] = [_short_id(value) for value in table[column].tolist()]
    st.dataframe(
        _styled_table(table),
        width="stretch",
        hide_index=True,
        column_config=_table_column_config(table),
    )


def _styled_table(frame: pd.DataFrame) -> pd.DataFrame | Styler:
    """Apply lightweight table styling for status cells."""
    status_columns = [column for column in frame.columns if _is_status_column(column)]
    if not status_columns:
        return frame
    return frame.style.map(_status_cell_style, subset=status_columns)


def _table_column_config(frame: pd.DataFrame, *, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build Streamlit column config for common audit table fields."""
    config = dict(extra or {})
    for column in frame.columns:
        if column in config:
            continue
        if column in _INTEGER_TABLE_COLUMNS:
            config[column] = st.column_config.NumberColumn(_humanize_column(column), format="%,d")
        elif column in _FLOAT_TABLE_COLUMNS:
            config[column] = st.column_config.NumberColumn(_humanize_column(column), format="%,.2f")
        elif _is_status_column(column):
            config[column] = st.column_config.TextColumn(_humanize_column(column), width="small")
    return config


def _is_status_column(column: object) -> bool:
    column_name = str(column)
    return column_name == "status" or column_name.endswith("_status")


def _status_cell_style(value: object) -> str:
    """Return supported dataframe cell styles for audit statuses."""
    status = str(value or "").strip().lower()
    if not status or status == "-":
        return ""
    if any(token in status for token in ("failed", "failure", "error", "rejected")):
        return "background-color: #fee2e2; color: #991b1b; font-weight: 700"
    if any(token in status for token in ("partial", "unsupported", "warn")):
        return "background-color: #ffedd5; color: #9a3412; font-weight: 700"
    if any(token in status for token in ("running", "queued", "pending")):
        return "background-color: #dbeafe; color: #1e40af; font-weight: 700"
    if any(token in status for token in ("completed", "success", "pass", "healthy")):
        return "background-color: #dcfce7; color: #166534; font-weight: 700"
    if "skipped" in status:
        return "background-color: #f3f4f6; color: #374151; font-weight: 700"
    return "background-color: #f3f4f6; color: #374151; font-weight: 600"


def _compact_run_frame(frame: pd.DataFrame, *, include_links: bool = False) -> pd.DataFrame:
    columns = [
        "started_at",
        "completed_at",
        "domain",
        "flow_name",
        "run_kind",
        "status",
        "duration_seconds",
        "units_total",
        "units_failed",
        "rows_written",
        "rows_rejected",
        "error_class",
        "error_message",
    ]
    available_columns = [column for column in columns if column in frame.columns]
    result = cast(pd.DataFrame, frame.loc[:, available_columns].copy())
    if include_links and "run_id" in frame.columns:
        detail_urls = [_run_detail_href(value) for value in frame["run_id"].tolist()]
        result.insert(0, "detail_url", pd.Series(detail_urls, index=result.index, dtype="string"))
        result.insert(1, "run", pd.Series([_short_id(value) for value in frame["run_id"].tolist()], index=result.index))
    if "duration_seconds" in result.columns:
        result["duration"] = [_format_duration(value) for value in result["duration_seconds"].tolist()]
        result = cast(pd.DataFrame, result.drop(columns=["duration_seconds"]))
    for column in ("started_at", "completed_at"):
        if column in result.columns:
            result[column] = [_format_datetime(value) for value in result[column].tolist()]
    if "error_message" in result.columns:
        result["error_message"] = [_truncate_text(value) for value in result["error_message"].tolist()]
    ordered_columns = [
        "detail_url",
        "run",
        "status",
        "domain",
        "flow_name",
        "run_kind",
        "started_at",
        "completed_at",
        "duration",
        "units_total",
        "units_failed",
        "rows_written",
        "rows_rejected",
        "error_class",
        "error_message",
    ]
    return cast(pd.DataFrame, result.loc[:, [column for column in ordered_columns if column in result.columns]])


def _compact_unit_frame(frame: pd.DataFrame, *, run_id: str) -> pd.DataFrame:
    columns = [
        "status",
        "unit_type",
        "unit_key_json",
        "reason",
        "rows_raw",
        "rows_valid",
        "rows_written",
        "rows_rejected",
        "duration_seconds",
        "source_uri",
        "error_class",
        "error_message",
    ]
    available_columns = [column for column in columns if column in frame.columns]
    result = cast(pd.DataFrame, frame.loc[:, available_columns].copy())
    if "unit_id" in frame.columns:
        detail_urls = [_unit_detail_href(run_id=run_id, unit_id=value) for value in frame["unit_id"].tolist()]
        result.insert(0, "unit_url", pd.Series(detail_urls, index=result.index, dtype="string"))
        unit_ids = [_short_id(value) for value in frame["unit_id"].tolist()]
        result.insert(1, "unit", pd.Series(unit_ids, index=result.index))
    unit_key_frame, unit_key_columns = _unit_key_columns(frame)
    if not unit_key_frame.empty:
        result = cast(pd.DataFrame, pd.concat([result, unit_key_frame], axis=1))
    if "unit_key_json" in result.columns:
        if not unit_key_columns:
            result["unit_key"] = [_format_json_compact(value) for value in result["unit_key_json"].tolist()]
        result = cast(pd.DataFrame, result.drop(columns=["unit_key_json"]))
    if "duration_seconds" in result.columns:
        result["duration"] = [_format_duration(value) for value in result["duration_seconds"].tolist()]
        result = cast(pd.DataFrame, result.drop(columns=["duration_seconds"]))
    if "error_message" in result.columns:
        result["error_message"] = [_truncate_text(value) for value in result["error_message"].tolist()]
    ordered_columns = [
        "unit_url",
        "unit",
        "status",
        "unit_type",
        *unit_key_columns,
        "unit_key",
        "reason",
        "rows_raw",
        "rows_valid",
        "rows_written",
        "rows_rejected",
        "duration",
        "source_uri",
        "error_class",
        "error_message",
    ]
    return cast(pd.DataFrame, result.loc[:, [column for column in ordered_columns if column in result.columns]])


def _current_route() -> dict[str, str | None]:
    page = _query_param("page") or _OVERVIEW_PAGE
    if page not in {_OVERVIEW_PAGE, _RUN_PAGE, _UNIT_PAGE}:
        page = _OVERVIEW_PAGE
    return {"page": page, "run_id": _query_param("run_id"), "unit_id": _query_param("unit_id")}


def _render_navigation(route: dict[str, str | None]) -> None:
    st.sidebar.subheader("Pages")
    if st.sidebar.button("Overview", width="stretch", disabled=route["page"] == _OVERVIEW_PAGE):
        _set_route(_OVERVIEW_PAGE)
        st.rerun()
    if route["page"] == _RUN_PAGE:
        st.sidebar.caption("Current page: Run Detail")
    elif route["page"] == _UNIT_PAGE:
        st.sidebar.caption("Current page: Unit Detail")
        if st.sidebar.button("Back to Run", width="stretch"):
            _set_route(_RUN_PAGE, run_id=route.get("run_id"))
            st.rerun()


def _set_route(page: str, *, run_id: str | None = None, unit_id: str | None = None) -> None:
    st.query_params["page"] = page
    if run_id:
        st.query_params["run_id"] = run_id
    elif "run_id" in st.query_params:
        del st.query_params["run_id"]
    if unit_id:
        st.query_params["unit_id"] = unit_id
    elif "unit_id" in st.query_params:
        del st.query_params["unit_id"]


def _query_param(name: str) -> str | None:
    value = st.query_params.get(name)
    if value is None:
        return None
    if isinstance(value, list):
        return str(value[0]) if value else None
    return str(value)


def _run_detail_href(run_id: object) -> str:
    return f"?page={_RUN_PAGE}&run_id={quote(str(run_id), safe='')}"


def _unit_detail_href(*, run_id: str, unit_id: object) -> str:
    return f"?page={_UNIT_PAGE}&run_id={quote(run_id, safe='')}&unit_id={quote(str(unit_id), safe='')}"


def _frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _domain_health_rows(
    *,
    latest_rows: list[dict[str, Any]],
    terminal_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build overview card rows from latest-attempt and terminal run snapshots."""
    latest_by_domain = {str(row.get("domain")): row for row in latest_rows if row.get("domain")}
    terminal_by_domain = {str(row.get("domain")): row for row in terminal_rows if row.get("domain")}
    domains = sorted(set(latest_by_domain) | set(terminal_by_domain))
    rows: list[dict[str, Any]] = []
    for domain in domains:
        latest = latest_by_domain.get(domain, {})
        terminal = terminal_by_domain.get(domain, {})
        latest_status = str(latest.get("status") or "missing")
        latest_time = latest.get("started_at") or latest.get("completed_at")
        terminal_time = terminal.get("completed_at")
        rows.append(
            {
                "domain": domain,
                "last_attempt_run_id": latest.get("run_id"),
                "last_attempt_status": _format_status(latest_status),
                "last_attempt_time": _format_datetime(latest_time),
                "last_attempt_flow": str(latest.get("flow_name") or "-"),
                "last_good_time": _format_datetime(terminal_time) if terminal else "No good run in window",
                "last_good_age": _format_age_since(terminal_time) if terminal else None,
            }
        )
    return rows


def _chunks(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [rows[index : index + size] for index in range(0, len(rows), size)]


def _attention_runs_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = _frame(rows)
    if frame.empty or "status" not in frame.columns:
        return pd.DataFrame()
    mask = frame["status"].astype("string").isin(tuple(_ATTENTION_STATUSES))
    return cast(pd.DataFrame, frame.loc[mask].copy())


def _selected_row_index(state: object) -> int | None:
    selection = getattr(state, "selection", None)
    if selection is None and isinstance(state, dict):
        selection = state.get("selection")
    rows = getattr(selection, "rows", None)
    if rows is None and isinstance(selection, dict):
        rows = selection.get("rows")
    if not rows:
        return None
    return int(cast(Any, rows)[0])


def _value_at(values: list[Any], index: int | None) -> Any | None:
    if index is None or index < 0 or index >= len(values):
        return None
    return values[index]


def _filter_frame_by_values(frame: pd.DataFrame, *, column: str, values: list[str]) -> pd.DataFrame:
    if column not in frame.columns:
        return frame
    if not values:
        return cast(pd.DataFrame, frame.iloc[0:0].copy())
    mask = frame[column].astype("string").isin(values)
    return cast(pd.DataFrame, frame.loc[mask].copy())


def _search_frame(frame: pd.DataFrame, *, query: str, columns: list[str]) -> pd.DataFrame:
    if not query:
        return frame
    available_columns = [column for column in columns if column in frame.columns]
    if not available_columns:
        return frame
    mask = pd.Series(False, index=frame.index)
    for column in available_columns:
        mask = mask | frame[column].astype("string").str.contains(query, case=False, na=False, regex=False)
    return cast(pd.DataFrame, frame.loc[mask].copy())


def _unit_key_columns(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    if "unit_key_json" not in frame.columns:
        return pd.DataFrame(index=frame.index), []

    unit_keys = [_json_dict(value) for value in frame["unit_key_json"].tolist()]
    key_names: list[str] = []
    for unit_key in unit_keys:
        for key in unit_key:
            column_name = _unit_key_column_name(key, existing_columns=frame.columns)
            if column_name not in key_names:
                key_names.append(column_name)

    if not key_names:
        return pd.DataFrame(index=frame.index), []

    rows: list[dict[str, str]] = []
    for unit_key in unit_keys:
        row: dict[str, str] = {}
        for key, value in unit_key.items():
            row[_unit_key_column_name(key, existing_columns=frame.columns)] = _format_key_value(value)
        rows.append(row)

    key_frame = pd.DataFrame(rows, index=frame.index)
    return key_frame, key_names


def _unit_key_column_name(key: object, *, existing_columns: pd.Index) -> str:
    column_name = str(key)
    if column_name in existing_columns:
        return f"key_{column_name}"
    return column_name


def _render_key_fields(value: object) -> None:
    unit_key = _json_dict(value)
    if not unit_key:
        st.info("No structured unit key recorded.")
        return

    for chunk in _chunks([{str(key): item} for key, item in unit_key.items()], 3):
        columns = st.columns(3)
        for column, item in zip(columns, chunk, strict=False):
            key, item_value = next(iter(item.items()))
            with column.container(border=True):
                st.caption(_humanize_column(key))
                st.markdown(f"**{_format_key_value(item_value)}**")


def _json_dict(value: object) -> dict[str, Any]:
    json_value = _json_value(value)
    if isinstance(json_value, dict):
        return {str(key): item for key, item in json_value.items()}
    return {}


def _render_run_status_callout(run: dict[str, Any]) -> None:
    status = str(run.get("status") or "").lower()
    error_class = run.get("error_class")
    error_message = run.get("error_message")
    if status == "failed":
        st.error(_error_text("Run failed", error_class=error_class, error_message=error_message))
    elif status == "partial":
        st.warning(_error_text("Run completed partially", error_class=error_class, error_message=error_message))
    elif status == "running":
        st.info("Run is still running. Stale detection uses the threshold in the sidebar.")
    elif status == "completed":
        st.success("Run completed without failed units or parser rejections.")
    elif status == "skipped":
        st.info("Run skipped because there was no eligible work.")


def _render_unit_status_callout(unit: dict[str, Any]) -> None:
    status = str(unit.get("status") or "").lower()
    error_class = unit.get("error_class")
    error_message = unit.get("error_message")
    if status == "failed":
        st.error(_error_text("Unit failed", error_class=error_class, error_message=error_message))
    elif status == "unsupported":
        st.warning(_error_text("Unit was unsupported", error_class=error_class, error_message=error_message))
    elif status == "skipped":
        st.info("Unit skipped. Check the reason and unit key before replaying work.")
    elif status == "completed":
        st.success("Unit completed.")


def _error_text(label: str, *, error_class: object, error_message: object) -> str:
    if error_class and error_message:
        return f"{label}: {error_class}: {error_message}"
    if error_class:
        return f"{label}: {error_class}"
    if error_message:
        return f"{label}: {error_message}"
    return label


def _render_limit_note(rows: list[dict[str, Any]], *, limit: int, label: str) -> None:
    if len(rows) >= limit:
        st.caption(f"Showing first {_format_int(limit)} {label}. Narrow the scope if you need the rest.")


def _format_window(hours: int) -> str:
    if hours < 24:
        return f"{hours} hours"
    days = hours // 24
    return f"{days} day{'s' if days != 1 else ''}"


def _format_int(value: object) -> str:
    if value is None:
        return "0"
    return f"{int(cast(Any, value)):,}"


def _format_status(value: object) -> str:
    status = str(value or "-")
    if status in _ATTENTION_STATUSES:
        return f"{status} needs attention"
    if status in _HEALTHY_RUN_STATUSES:
        return f"{status} healthy"
    return status


def _format_unit_count(run: dict[str, Any]) -> str:
    succeeded = run.get("units_succeeded")
    total = run.get("units_total")
    if total is None:
        return "0"
    if succeeded is None:
        return _format_int(total)
    return f"{_format_int(succeeded)}/{_format_int(total)}"


def _format_target_window(run: dict[str, Any]) -> str:
    start = run.get("target_window_start")
    end = run.get("target_window_end")
    if not start and not end:
        return ""
    if start and end:
        return f"target {start} to {end}"
    if start:
        return f"target from {start}"
    return f"target to {end}"


def _format_duration(value: object) -> str:
    if value is None:
        return "-"
    seconds = max(0, int(cast(Any, value)))
    minutes, remaining_seconds = divmod(seconds, 60)
    hours, remaining_minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {remaining_minutes}m"
    if minutes:
        return f"{minutes}m {remaining_seconds}s"
    return f"{remaining_seconds}s"


def _format_datetime(value: object) -> str:
    if value is None:
        return "-"
    if type(value).__name__ == "NaTType":
        return "-"
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
    return str(value)


def _format_age_since(value: object) -> str | None:
    if value is None or type(value).__name__ == "NaTType":
        return None
    if isinstance(value, datetime):
        delta_seconds = max(0, int((datetime.now(UTC) - value.astimezone(UTC)).total_seconds()))
        return f"{_format_duration(delta_seconds)} ago"
    return None


def _format_json_compact(value: object) -> str:
    if value is None:
        return "-"
    json_value = _json_value(value)
    if isinstance(json_value, dict | list):
        return json.dumps(json_value, default=str, separators=(",", ":"))
    return str(json_value)


def _format_key_value(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return _format_datetime(value)
    if isinstance(value, dict | list):
        return _format_json_compact(value)
    return str(value)


def _humanize_column(value: object) -> str:
    return str(value).replace("_", " ").strip().title()


def _truncate_text(value: object, *, limit: int = 96) -> str:
    if value is None or type(value).__name__ == "NaTType":
        return "-"
    text = str(value)
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1]}..."


def _safe_key(value: object) -> str:
    return "".join(character if character.isalnum() else "_" for character in str(value))


def _short_id(value: object) -> str:
    if value is None:
        return "-"
    text = str(value)
    if len(text) <= 12:
        return text
    return f"{text[:8]}...{text[-4:]}"


def _json_value(value: object) -> object:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return _jsonable(value)


def _jsonable(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _prefect_flow_run_url(prefect_flow_run_id: object) -> str | None:
    if not prefect_flow_run_id:
        return None
    base_url = os.getenv("PREFECT_UI_URL") or os.getenv("PREFECT_UI_API_URL", "").removesuffix("/api")
    if not base_url:
        return None
    return f"{base_url.rstrip('/')}/runs/flow-run/{prefect_flow_run_id}"


def _prefer_dashboard_motherduck_token() -> None:
    token = os.getenv("DASHBOARD_MOTHERDUCK_TOKEN", "").strip()
    if token:
        os.environ["MOTHERDUCK_TOKEN"] = token


if __name__ == "__main__":
    main()
