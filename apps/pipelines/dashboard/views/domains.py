"""Domains page for the pipeline audit dashboard."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.charts import render_evidence_summary, render_movement_charts
from dashboard.constants import ATTENTION_RUNS_LIMIT
from dashboard.domain_health import (
    at_risk_domain_rows,
    domain_rows_from_snapshot,
    render_domain_health_table,
    render_domain_metrics_strip,
)
from dashboard.filters import ScopeFilters, render_scope_controls, render_window_control
from dashboard.formatting import safe_key
from dashboard.loaders import load_snapshot
from dashboard.routing import (
    domains_href,
    landing_objects_href,
    overview_href,
    query_param,
    run_units_href,
    runs_href,
)
from dashboard.tables import frame, render_run_table
from dashboard.views.common import (
    lake_ready,
    load_or_show_error,
    render_cache_caption,
    render_page_header,
)


def render_domains_page() -> None:
    """Render the domain-focused audit route."""
    render_page_header(
        title="Domains",
        caption="Per-domain freshness, throughput, movement, and latest attention runs.",
        breadcrumb=(("Pipeline Audit", overview_href()), ("Domains", None)),
    )
    filters = render_scope_controls(
        key_prefix="domains",
        include_stale=False,
        domain_default=query_param("domain"),
    )
    snapshot = _load_snapshot(filters)
    if snapshot is None:
        return

    domain_rows = domain_rows_from_snapshot(snapshot)
    render_domain_metrics_strip(domain_rows=domain_rows, summary=snapshot["summary"])

    at_risk = at_risk_domain_rows(domain_rows)
    if at_risk and len(at_risk) < len(domain_rows):
        st.subheader("Needs attention")
        render_domain_health_table(at_risk, key="domains_at_risk")

    st.subheader("All domains")
    render_domain_health_table(domain_rows, key="domains_health")

    with st.expander("Movement and evidence", expanded=True):
        movement, evidence = st.tabs(["Movement", "Evidence"])
        with movement:
            render_movement_charts(snapshot)
        with evidence:
            render_evidence_summary(snapshot["evidence_summary"])

    st.subheader("Latest attention by domain")
    attention_frame = frame(snapshot["latest_attention_runs"])
    if attention_frame.empty:
        st.success("No failed or partial runs in the selected window.")
        return
    render_run_table(attention_frame, key="domains_attention_runs")


def render_domain_detail_page(domain: str | None) -> None:
    """Render one domain's drill-down route.

    Args:
        domain: Domain slug from query params.
    """
    render_page_header(
        title=f"Domain: {domain}" if domain else "Domain detail",
        caption="One-domain freshness, movement, evidence, and recent attention runs.",
        breadcrumb=(("Pipeline Audit", overview_href()), ("Domains", domains_href()), (str(domain or "Domain"), None)),
    )
    if not domain:
        st.warning("Choose a domain from the Domains page to open its detail page.")
        return

    since = render_window_control(key_prefix=f"domain_detail_{safe_key(domain)}")
    snapshot = load_or_show_error(
        lambda: load_snapshot(
            since_iso=since.isoformat(),
            stale_after_iso=since.isoformat(),
            domains=(domain,),
            attention_limit=ATTENTION_RUNS_LIMIT,
            include_triage=False,
        ),
        error_label="Domain detail",
    )
    if snapshot is None:
        return
    render_cache_caption(since)
    if not lake_ready(snapshot):
        return

    domain_rows = domain_rows_from_snapshot(snapshot)
    render_domain_metrics_strip(domain_rows=domain_rows, summary=snapshot["summary"])
    render_domain_health_table(domain_rows, key=f"domain_detail_health_{safe_key(domain)}")

    runs, units, landing = st.columns(3)
    runs.link_button("Runs", runs_href(domain=domain), icon=":material/history:", width="stretch")
    units.link_button("Run Units", run_units_href(domain=domain), icon=":material/view_list:", width="stretch")
    landing.link_button(
        "Landing Objects",
        landing_objects_href(domain=domain),
        icon=":material/cloud:",
        width="stretch",
    )

    with st.expander("Movement and evidence", expanded=True):
        movement, evidence = st.tabs(["Movement", "Evidence"])
        with movement:
            render_movement_charts(snapshot)
        with evidence:
            render_evidence_summary(snapshot["evidence_summary"])

    st.subheader("Latest attention runs")
    attention_frame = frame(snapshot["latest_attention_runs"])
    if attention_frame.empty:
        st.success("No failed or partial runs for this domain in the selected window.")
        return
    render_run_table(attention_frame, key=f"domain_detail_attention_{safe_key(domain)}")


def _load_snapshot(filters: ScopeFilters) -> dict[str, Any] | None:
    snapshot = load_or_show_error(
        lambda: load_snapshot(
            since_iso=filters["since"].isoformat(),
            stale_after_iso=filters["stale_after"].isoformat(),
            domains=tuple(filters["domains"]),
            attention_limit=ATTENTION_RUNS_LIMIT,
            include_triage=False,
        ),
        error_label="Domain audit data",
    )
    if snapshot is None:
        return None
    render_cache_caption(filters["since"])
    if not lake_ready(snapshot):
        return None
    return snapshot
