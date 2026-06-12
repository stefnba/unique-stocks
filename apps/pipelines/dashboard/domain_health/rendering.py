"""Streamlit rendering helpers for domain health views."""

from __future__ import annotations

from typing import Any

import streamlit as st

from dashboard.constants import LINK_COLUMN_LABEL_PATTERN
from dashboard.domain_health.model import domain_health_dataframe, domain_status_counts
from dashboard.formatting import format_int
from dashboard.tables import styled_table, table_column_config


def render_domain_metrics_strip(
    *,
    domain_rows: list[dict[str, Any]],
    summary: dict[str, int],
) -> None:
    """Render domain-level KPI metrics."""
    counts = domain_status_counts(domain_rows)
    columns = st.columns(6)
    columns[0].metric("Domains", format_int(len(domain_rows)))
    columns[1].metric(
        "Need attention",
        format_int(counts.get("attention", 0) + counts.get("recent attention", 0)),
    )
    columns[2].metric("Running", format_int(counts.get("running", 0)))
    columns[3].metric("No good run", format_int(counts.get("no good run", 0)))
    columns[4].metric("Rows rejected", format_int(summary["rows_rejected"]))
    columns[5].metric("Rows written", format_int(summary["rows_written"]))


def render_domain_health_table(rows: list[dict[str, Any]], *, key: str) -> None:
    """Render the per-domain health matrix."""
    table = domain_health_dataframe(rows)
    if table.empty:
        st.info("No runs found for the selected domains.")
        return
    st.dataframe(
        styled_table(table),
        width="stretch",
        hide_index=True,
        key=key,
        column_config=table_column_config(
            table,
            extra={
                "latest_run": st.column_config.LinkColumn(
                    "Latest run",
                    width="small",
                    display_text=LINK_COLUMN_LABEL_PATTERN,
                ),
                "attention_run": st.column_config.LinkColumn(
                    "Attention run",
                    width="small",
                    display_text=LINK_COLUMN_LABEL_PATTERN,
                ),
                "browse_runs": st.column_config.LinkColumn(
                    "Runs",
                    width="small",
                    display_text=LINK_COLUMN_LABEL_PATTERN,
                ),
                "domain": st.column_config.LinkColumn(
                    "Domain",
                    width="medium",
                    display_text=LINK_COLUMN_LABEL_PATTERN,
                ),
            },
        ),
    )
    st.caption("Click a domain for detail, or open latest and attention run IDs for run investigation.")
