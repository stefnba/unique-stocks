"""Domain health row building and table rendering for the audit dashboard."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import streamlit as st

from dashboard.constants import LINK_COLUMN_LABEL_PATTERN
from dashboard.formatting import format_age_since, format_int, short_id
from dashboard.routing import domain_detail_href, labeled_href, run_detail_href, runs_href
from dashboard.tables import styled_table, table_column_config

AT_RISK_DOMAIN_STATUSES = frozenset({"attention", "recent attention", "no good run"})


def domain_rows_from_snapshot(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """Build domain health rows from a :func:`load_snapshot` result."""
    return build_domain_health_rows(
        latest_rows=snapshot["latest_runs"],
        terminal_rows=snapshot["latest_terminal_runs"],
        attention_rows=snapshot["latest_attention_runs"],
        summary_rows=snapshot["domain_summary"],
    )


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


def build_domain_health_rows(
    *,
    latest_rows: list[dict[str, Any]],
    terminal_rows: list[dict[str, Any]],
    attention_rows: list[dict[str, Any]],
    summary_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build sorted domain health rows from lake snapshot slices.

    Args:
        latest_rows: Most recent run per domain regardless of status.
        terminal_rows: Most recent healthy terminal run per domain in the window.
        attention_rows: Most recent failed or partial run per domain in the window.
        summary_rows: Window-level per-domain run and row counters.

    Returns:
        Ledger-ready rows keyed by domain with status and compact counters.
    """
    latest_by_domain = {str(row.get("domain")): row for row in latest_rows if row.get("domain")}
    terminal_by_domain = {str(row.get("domain")): row for row in terminal_rows if row.get("domain")}
    attention_by_domain = {str(row.get("domain")): row for row in attention_rows if row.get("domain")}
    summary_by_domain = {str(row.get("domain")): row for row in summary_rows if row.get("domain")}
    domains = sorted(
        set(latest_by_domain) | set(terminal_by_domain) | set(attention_by_domain) | set(summary_by_domain)
    )
    rows: list[dict[str, Any]] = []
    for domain in domains:
        latest = latest_by_domain.get(domain, {})
        terminal = terminal_by_domain.get(domain, {})
        attention = attention_by_domain.get(domain, {})
        summary = summary_by_domain.get(domain, {})
        latest_status = str(latest.get("status") or "missing")
        latest_time = latest.get("started_at") or latest.get("completed_at")
        terminal_time = terminal.get("completed_at")
        latest_run_id = latest.get("run_id")
        attention_run_id = attention.get("run_id")
        prior_attention = attention if attention_run_id and attention_run_id != latest_run_id else {}
        prior_attention_time = prior_attention.get("started_at") or prior_attention.get("completed_at")
        prior_attention_run_id = prior_attention.get("run_id")
        rows.append(
            {
                "domain": domain,
                "health_status": domain_health_status(
                    latest_status=latest_status,
                    terminal=terminal,
                    attention=attention,
                ),
                "latest_at": compact_datetime(latest_time),
                "latest_flow": str(latest.get("flow_name") or "-"),
                "latest_run_href": run_detail_href(latest_run_id) if latest_run_id else "",
                "latest_run_label": short_id(latest_run_id),
                "last_good": last_good_label(terminal_time) if terminal else "no good",
                "attention_at": compact_datetime(prior_attention_time) if prior_attention else "-",
                "attention_run_href": run_detail_href(prior_attention_run_id) if prior_attention_run_id else "",
                "attention_run_label": short_id(prior_attention_run_id),
                "browse_runs_href": runs_href(domain=domain),
                "runs": int(summary.get("runs") or 0),
                "running_runs": int(summary.get("running_runs") or 0),
                "attention_runs": int(summary.get("attention_runs") or 0),
                "units_failed": int(summary.get("units_failed") or 0),
                "rows_written": int(summary.get("rows_written") or 0),
                "rows_rejected": int(summary.get("rows_rejected") or 0),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            domain_sort_rank(row["health_status"]),
            -int(row["attention_runs"]),
            -int(row["running_runs"]),
            -int(row["rows_rejected"]),
            str(row["domain"]),
        ),
    )


def domain_health_status(
    *,
    latest_status: str,
    terminal: dict[str, Any],
    attention: dict[str, Any],
) -> str:
    """Return one scannable domain health label."""
    if latest_status in {"failed", "partial"}:
        return "attention"
    if latest_status == "running":
        return "running"
    if attention:
        return "recent attention"
    if not terminal:
        return "no good run"
    return "healthy"


def domain_sort_rank(status: object) -> int:
    """Return sort priority for domain health labels."""
    ranks = {
        "attention": 0,
        "no good run": 1,
        "recent attention": 2,
        "running": 3,
        "healthy": 4,
    }
    return ranks.get(str(status), 5)


def last_good_label(value: object) -> str:
    """Format latest healthy completion as a compact age label."""
    age = format_age_since(value)
    if not age:
        return compact_datetime(value)
    return age


def compact_datetime(value: object) -> str:
    """Format a timestamp for dense domain table cells."""
    if value is None or type(value).__name__ == "NaTType":
        return "-"
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%m-%d %H:%M")
    text = str(value)
    if len(text) >= 16 and text[4:5] == "-" and text[7:8] == "-":
        return text[5:16]
    return text.removesuffix(" UTC")


def at_risk_domain_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return domain rows that need operator attention."""
    return [row for row in rows if str(row["health_status"]) in AT_RISK_DOMAIN_STATUSES]


def domain_health_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert domain health rows into a display-ready dataframe."""
    if not rows:
        return pd.DataFrame()
    records: list[dict[str, Any]] = []
    for row in rows:
        latest_href = str(row.get("latest_run_href") or "").strip()
        attention_href = str(row.get("attention_run_href") or "").strip()
        records.append(
            {
                "domain": labeled_href(domain_detail_href(row["domain"]), str(row["domain"])),
                "health_status": row["health_status"],
                "last_good": row["last_good"],
                "latest_at": row["latest_at"],
                "latest_run": (
                    labeled_href(latest_href, str(row["latest_run_label"]))
                    if latest_href and str(row["latest_run_label"]) != "-"
                    else "-"
                ),
                "latest_flow": row["latest_flow"],
                "attention_at": row["attention_at"],
                "attention_run": (
                    labeled_href(attention_href, str(row["attention_run_label"]))
                    if attention_href and str(row["attention_run_label"]) != "-"
                    else "-"
                ),
                "runs": row["runs"],
                "units_failed": row["units_failed"],
                "rows_rejected": row["rows_rejected"],
                "rows_written": row["rows_written"],
                "browse_runs": labeled_href(str(row["browse_runs_href"]), "runs"),
            }
        )
    columns = [
        "domain",
        "health_status",
        "last_good",
        "latest_at",
        "latest_run",
        "latest_flow",
        "attention_at",
        "attention_run",
        "runs",
        "units_failed",
        "rows_rejected",
        "rows_written",
        "browse_runs",
    ]
    return pd.DataFrame(records, columns=pd.Index(columns))


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


def domain_status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Count domains by health status label."""
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row["health_status"])
        counts[status] = counts.get(status, 0) + 1
    return counts


def attention_domain_count(rows: list[dict[str, Any]]) -> int:
    """Return how many domains need operator attention."""
    counts = domain_status_counts(rows)
    return counts.get("attention", 0) + counts.get("recent attention", 0) + counts.get("no good run", 0)
