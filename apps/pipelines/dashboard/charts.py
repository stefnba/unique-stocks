"""Shared chart sections for dashboard pages."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.formatting import format_int
from dashboard.tables import frame


def render_evidence_summary(evidence_summary: dict[str, int]) -> None:
    """Render cross-table audit evidence counters."""
    columns = st.columns(5)
    columns[0].metric("Landing objects", format_int(evidence_summary["landing_objects"]))
    columns[1].metric("Landing bytes", format_int(evidence_summary["landing_bytes"]))
    columns[2].metric("Rejections", format_int(evidence_summary["rejection_samples"]))
    columns[3].metric("dbt invocations", format_int(evidence_summary["dbt_invocations"]))
    columns[4].metric("dbt attention nodes", format_int(evidence_summary["dbt_attention_nodes"]))


def render_movement_charts(snapshot: dict[str, Any]) -> None:
    """Render run volume and row throughput charts for the selected window."""
    trend, status, throughput = st.columns([2, 1, 2])
    trend_frame = frame(snapshot["daily_trend"])
    with trend:
        st.markdown("**Daily runs by domain**")
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

    with throughput:
        st.markdown("**Rows written vs rejected**")
        if trend_frame.empty:
            st.info("No row movement in the selected window.")
        else:
            st.bar_chart(trend_frame, x="run_date", y=["rows_written", "rows_rejected"])
