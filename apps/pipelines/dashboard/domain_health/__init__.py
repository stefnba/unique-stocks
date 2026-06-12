"""Public domain health facade for the dashboard."""

from __future__ import annotations

from dashboard.domain_health.model import (
    AT_RISK_DOMAIN_STATUSES,
    at_risk_domain_rows,
    attention_domain_count,
    build_domain_health_rows,
    compact_datetime,
    domain_health_dataframe,
    domain_health_status,
    domain_rows_from_snapshot,
    domain_sort_rank,
    domain_status_counts,
    last_good_label,
)
from dashboard.domain_health.rendering import render_domain_health_table, render_domain_metrics_strip

__all__ = [
    "AT_RISK_DOMAIN_STATUSES",
    "at_risk_domain_rows",
    "attention_domain_count",
    "build_domain_health_rows",
    "compact_datetime",
    "domain_health_dataframe",
    "domain_health_status",
    "domain_rows_from_snapshot",
    "domain_sort_rank",
    "domain_status_counts",
    "last_good_label",
    "render_domain_health_table",
    "render_domain_metrics_strip",
]
