"""Streamlit entrypoint for monitoring pipeline audit runs."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict, cast
from urllib.parse import quote

import pandas as pd
import streamlit as st

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
    load_run_units,
    load_stale_running_runs,
    load_status_breakdown,
    load_status_summary,
    load_unit_status_breakdown,
    pipeline_runs_available,
)

_CACHE_TTL_SECONDS = 60
_OVERVIEW_PAGE = "overview"
_RUN_PAGE = "run"


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

    _render_overview_page()


def _render_overview_page() -> None:
    """Render the run monitoring overview route."""
    st.title("Pipeline Runs")
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
    _render_alerts(snapshot)
    _render_overview(snapshot)
    _render_recent_runs(snapshot["recent_runs"])


def _render_run_page(run_id: str | None) -> None:
    """Render one run's drill-down route."""
    st.title("Run Detail")

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
    _render_run_detail(page["detail"])


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
                "run_units": load_run_units(lake, run_id=run_id),
                "landing_objects": load_landing_objects(lake, run_id=run_id),
                "rejections": load_rejections(lake, run_id=run_id),
                "dbt_node_results": load_dbt_node_results(lake, run_id=run_id),
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


def _render_alerts(snapshot: dict[str, Any]) -> None:
    stale_runs = snapshot["stale_runs"]
    summary = snapshot["summary"]
    if stale_runs:
        st.error(f"{len(stale_runs)} run(s) are stale in running state.")
        _render_run_table(_frame(stale_runs))
    elif summary["attention_runs"]:
        st.warning(f"{summary['attention_runs']} failed or partial run(s) in the selected window.")
    else:
        st.success("No failed, partial, or stale runs in the selected window.")


def _render_overview(snapshot: dict[str, Any]) -> None:
    latest, freshness = st.tabs(["Latest", "Freshness"])
    with latest:
        latest_runs = _frame(snapshot["latest_runs"])
        if latest_runs.empty:
            st.info("No runs found for the selected domains.")
        else:
            _render_run_table(latest_runs)

    with freshness:
        terminal_runs = _frame(snapshot["latest_terminal_runs"])
        if terminal_runs.empty:
            st.info("No healthy terminal runs found in the selected window.")
        else:
            _render_run_table(terminal_runs)

    trend, status = st.columns(2)
    with trend:
        st.subheader("Daily Runs")
        trend_frame = _frame(snapshot["daily_trend"])
        if trend_frame.empty:
            st.info("No trend data in the selected window.")
        else:
            st.line_chart(trend_frame, x="run_date", y="runs", color="domain")

    with status:
        st.subheader("Status Mix")
        status_frame = _frame(snapshot["status_breakdown"])
        if status_frame.empty:
            st.info("No status data in the selected window.")
        else:
            st.bar_chart(status_frame, x="status", y="runs", color="status")


def _render_recent_runs(rows: list[dict[str, Any]]) -> None:
    st.subheader("Recent Runs")
    if not rows:
        st.info("No recent runs match the selected filters.")
        return

    run_frame = _frame(rows)
    _render_run_table(run_frame)
    st.caption("Open a run from the table to view units, landing objects, rejections, and dbt node results.")


def _render_run_summary(run: dict[str, Any]) -> None:
    st.subheader(str(run["flow_name"]))
    left, middle, right, rows = st.columns(4)
    left.metric("Status", str(run["status"]))
    middle.metric("Duration", _format_duration(run.get("duration_seconds")))
    right.metric("Rows Written", _format_int(run.get("rows_written")))
    rows.metric("Rows Rejected", _format_int(run.get("rows_rejected")))

    prefect_url = _prefect_flow_run_url(run.get("prefect_flow_run_id"))
    if prefect_url:
        st.link_button("Open in Prefect", prefect_url)

    run_metadata = {
        "run_id": run.get("run_id"),
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
    with st.expander("Run Metadata"):
        st.json(_jsonable(run_metadata))
    with st.expander("Parameters"):
        st.json(_json_value(run.get("parameters_json")))
    with st.expander("Summary"):
        st.json(_json_value(run.get("summary_json")))


def _render_run_detail(detail: dict[str, list[dict[str, Any]]]) -> None:
    units, landing, rejections, dbt = st.tabs(["Units", "Landing", "Rejections", "dbt"])
    with units:
        status_frame = _frame(detail["unit_status"])
        if not status_frame.empty:
            st.bar_chart(status_frame, x="status", y="units", color="status")
        unit_frame = _frame(detail["run_units"])
        if unit_frame.empty:
            st.info("No work units recorded for this run.")
        else:
            st.dataframe(unit_frame, width="stretch", hide_index=True)

    with landing:
        landing_frame = _frame(detail["landing_objects"])
        if landing_frame.empty:
            st.info("No landing objects recorded for this run.")
        else:
            st.dataframe(landing_frame, width="stretch", hide_index=True)

    with rejections:
        rejection_frame = _frame(detail["rejections"])
        if rejection_frame.empty:
            st.info("No sampled rejections recorded for this run.")
        else:
            st.dataframe(rejection_frame, width="stretch", hide_index=True)

    with dbt:
        dbt_frame = _frame(detail["dbt_node_results"])
        if dbt_frame.empty:
            st.info("No dbt node results recorded for this run.")
        else:
            st.dataframe(dbt_frame, width="stretch", hide_index=True)


def _render_run_table(frame: pd.DataFrame) -> None:
    table = _compact_run_frame(frame, include_links=True)
    st.dataframe(
        table,
        width="stretch",
        hide_index=True,
        column_config={
            "detail_url": st.column_config.LinkColumn("Detail", display_text="Open"),
        },
    )


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
    if "duration_seconds" in result.columns:
        result["duration"] = [_format_duration(value) for value in result["duration_seconds"].tolist()]
        result = cast(pd.DataFrame, result.drop(columns=["duration_seconds"]))
    return result


def _current_route() -> dict[str, str | None]:
    page = _query_param("page") or _OVERVIEW_PAGE
    if page not in {_OVERVIEW_PAGE, _RUN_PAGE}:
        page = _OVERVIEW_PAGE
    return {"page": page, "run_id": _query_param("run_id")}


def _render_navigation(route: dict[str, str | None]) -> None:
    st.sidebar.subheader("Pages")
    if st.sidebar.button("Overview", width="stretch", disabled=route["page"] == _OVERVIEW_PAGE):
        _set_route(_OVERVIEW_PAGE)
        st.rerun()
    if route["page"] == _RUN_PAGE:
        st.sidebar.button("Run Detail", width="stretch", disabled=True)


def _set_route(page: str, *, run_id: str | None = None) -> None:
    st.query_params["page"] = page
    if run_id:
        st.query_params["run_id"] = run_id
    elif "run_id" in st.query_params:
        del st.query_params["run_id"]


def _query_param(name: str) -> str | None:
    value = st.query_params.get(name)
    if value is None:
        return None
    if isinstance(value, list):
        return str(value[0]) if value else None
    return str(value)


def _run_detail_href(run_id: object) -> str:
    return f"?page={_RUN_PAGE}&run_id={quote(str(run_id), safe='')}"


def _frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _format_window(hours: int) -> str:
    if hours < 24:
        return f"{hours} hours"
    days = hours // 24
    return f"{days} day{'s' if days != 1 else ''}"


def _format_int(value: object) -> str:
    if value is None:
        return "0"
    return f"{int(cast(Any, value)):,}"


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
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
    return str(value)


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
