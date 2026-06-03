"""Overview page for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.constants import ATTENTION_RUNS_LIMIT, CACHE_TTL_SECONDS
from dashboard.formatting import format_age_since, format_datetime, format_int, format_status
from dashboard.loaders import load_snapshot
from dashboard.routing import render_breadcrumb, run_detail_href
from dashboard.tables import frame, render_run_table, search_frame
from dashboard.views.sidebar import render_sidebar


def render_overview_page() -> None:
    """Render the run monitoring overview route.

    Loads the cached overview snapshot, renders KPI/triage/domain sections,
    and exposes collapsed activity and lookup explorers.
    """
    render_breadcrumb(("Pipeline Audit", None))
    st.title("Pipeline Audit")
    st.caption("Monitor data-plane run health, then drill into the exact units and evidence for any suspect run.")
    filters = render_sidebar()

    try:
        snapshot = load_snapshot(
            since_iso=filters["since"].isoformat(),
            stale_after_iso=filters["stale_after"].isoformat(),
            domains=tuple(filters["domains"]),
            statuses=tuple(filters["statuses"]),
            recent_limit=int(filters["recent_limit"]),
            attention_limit=ATTENTION_RUNS_LIMIT,
        )
    except Exception as exc:
        st.error("Lake audit data is not reachable.")
        st.caption(f"{type(exc).__name__}: {exc}")
        return

    st.caption(f"Window starts {format_datetime(filters['since'])}. Cached for {CACHE_TTL_SECONDS} seconds.")

    if not snapshot["available"]:
        st.warning("pipeline.runs is not available in the configured lake.")
        return

    _render_kpis(snapshot["summary"], snapshot["stale_runs"])
    _render_triage(snapshot)
    _render_domain_status(snapshot)
    _render_activity(snapshot)
    _render_run_lookup(
        snapshot["recent_runs"],
        attention_runs=snapshot["attention_runs"],
        stale_runs=snapshot["stale_runs"],
    )


def _render_kpis(summary: dict[str, int], stale_runs: list[dict[str, Any]]) -> None:
    """Render top-level overview KPI metrics.

    Args:
        summary: Run-level KPI counters for the selected window.
        stale_runs: Runs currently flagged as stale in ``running`` state.
    """
    columns = st.columns(6)
    columns[0].metric("Runs", format_int(summary["total_runs"]))
    columns[1].metric("Attention", format_int(summary["attention_runs"]))
    columns[2].metric("Running", format_int(summary["running_runs"]))
    columns[3].metric("Stale", format_int(len(stale_runs)))
    columns[4].metric("Rows Written", format_int(summary["rows_written"]))
    columns[5].metric("Rows Rejected", format_int(summary["rows_rejected"]))


def _render_triage(snapshot: dict[str, Any]) -> None:
    """Render the action queue for stale and attention runs.

    Args:
        snapshot: Overview snapshot containing ``stale_runs`` and ``attention_runs``.
    """
    st.subheader("Triage")
    st.caption("Start here when something needs investigation.")
    stale_runs = snapshot["stale_runs"]
    attention_runs = snapshot["attention_runs"]

    if not stale_runs and not attention_runs:
        st.success("No failed, partial, or stale runs in the selected window.")
        return

    if stale_runs:
        st.error(f"{len(stale_runs)} run(s) are stale in running state.")
        render_run_table(frame(stale_runs), key="stale_runs")

    if attention_runs:
        st.warning(f"{len(attention_runs)} failed or partial run(s) in the selected window.")
        render_run_table(frame(attention_runs), key="attention_runs")


def _render_domain_status(snapshot: dict[str, Any]) -> None:
    """Render per-domain health cards for the overview page.

    Args:
        snapshot: Overview snapshot containing latest, terminal, and attention run rows.
    """
    st.subheader("Domain Status")
    domain_rows = _domain_health_rows(
        latest_rows=snapshot["latest_runs"],
        terminal_rows=snapshot["latest_terminal_runs"],
        attention_rows=snapshot["latest_attention_runs"],
    )
    if not domain_rows:
        st.info("No runs found for the selected domains.")
        return

    for chunk in _chunks(domain_rows, 3):
        columns = st.columns(3)
        for column, row in zip(columns, chunk, strict=False):
            with column.container(border=True):
                st.markdown(f"**{row['domain']}**")
                st.caption(f"Most recent run: {row['last_attempt_status']}")
                if row["attention_status"]:
                    st.warning(
                        f"Latest attention in window: {row['attention_status']} "
                        f"({row['attention_time']}) · {row['attention_flow']}"
                    )
                    if row["attention_run_id"]:
                        st.link_button(
                            "Open attention run",
                            run_detail_href(row["attention_run_id"]),
                            width="stretch",
                        )
                if row["last_good_age"]:
                    st.caption("Last good run")
                    st.markdown(f"**{row['last_good_time']}**")
                    st.caption(row["last_good_age"])
                else:
                    st.caption("Last good run")
                    st.warning(row["last_good_time"])
                st.caption(f"Attempted {row['last_attempt_time']} · {row['last_attempt_flow']}")
                if row["last_attempt_run_id"]:
                    st.link_button("Open latest run", run_detail_href(row["last_attempt_run_id"]), width="stretch")


def _render_activity(snapshot: dict[str, Any]) -> None:
    """Render collapsed activity charts for the selected window.

    Args:
        snapshot: Overview snapshot containing trend and status breakdown rows.
    """
    with st.expander("Activity", expanded=False):
        trend, status = st.columns(2)
        with trend:
            st.markdown("**Daily runs**")
            trend_frame = frame(snapshot["daily_trend"])
            if trend_frame.empty:
                st.info("No trend data in the selected window.")
            else:
                st.line_chart(trend_frame, x="run_date", y="runs", color="domain")

        with status:
            st.markdown("**Status mix**")
            status_frame = frame(snapshot["status_breakdown"])
            if status_frame.empty:
                st.info("No status data in the selected window.")
            else:
                st.bar_chart(status_frame, x="status", y="runs", color="status")


def _render_run_lookup(
    rows: list[dict[str, Any]],
    *,
    attention_runs: list[dict[str, Any]],
    stale_runs: list[dict[str, Any]],
) -> None:
    """Render the searchable lookup explorer for non-triage runs.

    Args:
        rows: Recent runs loaded for the lookup explorer.
        attention_runs: Runs already shown in the triage queue.
        stale_runs: Runs already shown in the stale queue.
    """
    excluded_ids = {str(row.get("run_id")) for row in (*attention_runs, *stale_runs) if row.get("run_id")}
    lookup_rows = [row for row in rows if str(row.get("run_id")) not in excluded_ids]

    with st.expander("Run Lookup", expanded=False):
        st.caption("Search and browse runs. Attention-queue runs are shown in Triage above.")
        search = str(
            st.text_input(
                "Search runs",
                placeholder="Run id, flow name, domain, error class, or message",
                key="run_lookup_search",
            )
        ).strip()
        lookup_frame = frame(lookup_rows)
        if lookup_frame.empty:
            st.info("No additional runs match the selected lookup filters.")
            return

        lookup_frame = search_frame(
            lookup_frame,
            query=search,
            columns=["run_id", "flow_name", "domain", "status", "error_class", "error_message", "run_kind"],
        )
        if lookup_frame.empty:
            st.info("No runs match the search query.")
            return

        render_run_table(lookup_frame, key="run_lookup")


def _domain_health_rows(
    *,
    latest_rows: list[dict[str, Any]],
    terminal_rows: list[dict[str, Any]],
    attention_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build domain-card rows from latest, terminal, and attention snapshots.

    Args:
        latest_rows: Most recent run per domain regardless of status.
        terminal_rows: Most recent healthy terminal run per domain in the window.
        attention_rows: Most recent failed or partial run per domain in the window.

    Returns:
        Card-ready rows keyed by domain with attempt, attention, and freshness fields.
    """
    latest_by_domain = {str(row.get("domain")): row for row in latest_rows if row.get("domain")}
    terminal_by_domain = {str(row.get("domain")): row for row in terminal_rows if row.get("domain")}
    attention_by_domain = {str(row.get("domain")): row for row in attention_rows if row.get("domain")}
    domains = sorted(set(latest_by_domain) | set(terminal_by_domain) | set(attention_by_domain))
    rows: list[dict[str, Any]] = []
    for domain in domains:
        latest = latest_by_domain.get(domain, {})
        terminal = terminal_by_domain.get(domain, {})
        attention = attention_by_domain.get(domain, {})
        latest_status = str(latest.get("status") or "missing")
        latest_time = latest.get("started_at") or latest.get("completed_at")
        terminal_time = terminal.get("completed_at")
        attention_time = attention.get("started_at") or attention.get("completed_at")
        latest_run_id = latest.get("run_id")
        attention_run_id = attention.get("run_id")
        attention_is_older = attention_run_id != latest_run_id
        rows.append(
            {
                "domain": domain,
                "last_attempt_run_id": latest_run_id,
                "last_attempt_status": format_status(latest_status),
                "last_attempt_time": format_datetime(latest_time),
                "last_attempt_flow": str(latest.get("flow_name") or "-"),
                "attention_run_id": attention_run_id if attention_is_older else None,
                "attention_status": format_status(attention.get("status")) if attention_is_older else None,
                "attention_time": format_datetime(attention_time) if attention_is_older else None,
                "attention_flow": str(attention.get("flow_name") or "-") if attention_is_older else None,
                "last_good_time": format_datetime(terminal_time) if terminal else "No good run in window",
                "last_good_age": format_age_since(terminal_time) if terminal else None,
            }
        )
    return rows


def _chunks(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    """Split a list into fixed-size chunks for card layout columns.

    Args:
        rows: Source rows to chunk.
        size: Maximum chunk length.

    Returns:
        List of row chunks.
    """
    return [rows[index : index + size] for index in range(0, len(rows), size)]
