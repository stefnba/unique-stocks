"""Overview page for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.charts import render_evidence_summary, render_movement_charts
from dashboard.constants import ATTENTION_RUNS_LIMIT
from dashboard.domain_health import (
    at_risk_domain_rows,
    attention_domain_count,
    domain_rows_from_snapshot,
    render_domain_health_table,
)
from dashboard.filters import ScopeFilters, render_scope_controls
from dashboard.formatting import format_int
from dashboard.loaders import load_snapshot
from dashboard.routing import domains_href, runs_href
from dashboard.tables import frame, merge_action_queue, render_action_queue
from dashboard.views.common import (
    lake_ready,
    load_or_show_error,
    render_cache_caption,
    render_page_header,
)


def render_overview_page() -> None:
    """Render the operator home route: triage first, then links to deeper views."""
    render_page_header(
        title="Overview",
        caption="Runs and domains that need attention. Open Domains or Runs for full views.",
        breadcrumb=(("Pipeline Audit", None),),
    )
    filters = render_scope_controls(key_prefix="overview", include_stale=True)
    snapshot = _load_snapshot(filters)
    if snapshot is None:
        return

    domain_rows = domain_rows_from_snapshot(snapshot)
    _render_status_strip(snapshot["summary"], snapshot["stale_runs"], domain_rows)
    _render_action_queue(snapshot)
    _render_domain_preview(domain_rows)
    _render_secondary_sections(snapshot)


def _load_snapshot(filters: ScopeFilters) -> dict[str, Any] | None:
    snapshot = load_or_show_error(
        lambda: load_snapshot(
            since_iso=filters["since"].isoformat(),
            stale_after_iso=filters["stale_after"].isoformat(),
            domains=tuple(filters["domains"]),
            attention_limit=ATTENTION_RUNS_LIMIT,
            include_triage=True,
        ),
        error_label="Lake audit data",
    )
    if snapshot is None:
        return None
    render_cache_caption(filters["since"])
    if not lake_ready(snapshot):
        return None
    return snapshot


def _render_status_strip(
    summary: dict[str, int],
    stale_runs: list[dict[str, Any]],
    domain_rows: list[dict[str, Any]],
) -> None:
    """Render compact operator metrics."""
    columns = st.columns(5)
    columns[0].metric("Attention runs", format_int(summary["attention_runs"]))
    columns[1].metric("Stale running", format_int(len(stale_runs)))
    columns[2].metric("Domains at risk", format_int(attention_domain_count(domain_rows)))
    columns[3].metric("Rows rejected", format_int(summary["rows_rejected"]))
    columns[4].metric("Running", format_int(summary["running_runs"]))


def _render_action_queue(snapshot: dict[str, Any]) -> None:
    """Render one combined queue for stale and failed runs."""
    st.subheader("Action queue")
    queue_rows = merge_action_queue(snapshot["stale_runs"], snapshot["attention_runs"])
    if not queue_rows:
        st.success("No failed, partial, or stale runs in the selected window.")
        return

    stale_count = len(snapshot["stale_runs"])
    attention_only = len(queue_rows) - stale_count
    if stale_count and attention_only:
        st.caption(f"{format_int(stale_count)} stale · {format_int(attention_only)} failed or partial")
    elif stale_count:
        st.caption(f"{format_int(stale_count)} stale running run(s)")
    else:
        st.caption(f"{format_int(attention_only)} failed or partial run(s)")

    render_action_queue(frame(queue_rows), key="action_queue")


def _render_domain_preview(domain_rows: list[dict[str, Any]]) -> None:
    """Show at-risk domains on overview with a handoff to the domains page."""
    at_risk = at_risk_domain_rows(domain_rows)
    header_left, header_right = st.columns([3, 1])
    header_left.subheader("Domains at risk")
    header_right.link_button("All domains", domains_href(), width="stretch")

    if not at_risk:
        st.success("All tracked domains look healthy.")
        return

    st.caption(f"{format_int(len(at_risk))} of {format_int(len(domain_rows))} domain(s) need attention.")
    render_domain_health_table(at_risk, key="overview_at_risk_domains")


def _render_secondary_sections(snapshot: dict[str, Any]) -> None:
    """Render optional movement and evidence sections below the fold."""
    with st.expander("Movement and evidence", expanded=False):
        movement, evidence = st.tabs(["Movement", "Evidence"])
        with movement:
            render_movement_charts(snapshot)
        with evidence:
            render_evidence_summary(snapshot["evidence_summary"])

    st.link_button("Browse all runs", runs_href(), width="content")
