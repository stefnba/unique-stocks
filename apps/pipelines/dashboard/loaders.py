"""Cached lake loaders for the pipeline audit dashboard."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import streamlit as st

from core.clients.lake import DataLakeClient
from dashboard.constants import (
    CACHE_TTL_SECONDS,
    DBT_NODE_RESULTS_LIMIT,
    LANDING_OBJECTS_LIMIT,
    REJECTIONS_LIMIT,
    RUN_UNITS_LIMIT,
)
from dashboard.queries import (
    load_attention_runs,
    load_daily_run_trend,
    load_dbt_node_results,
    load_landing_objects,
    load_latest_attention_runs_by_domain,
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


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_snapshot(
    *,
    since_iso: str,
    stale_after_iso: str,
    domains: tuple[str, ...],
    statuses: tuple[str, ...],
    recent_limit: int,
    attention_limit: int,
) -> dict[str, Any]:
    """Load all overview-page lake data for the selected filters.

    Args:
        since_iso: ISO timestamp for the start of the dashboard window.
        stale_after_iso: ISO timestamp before which ``running`` runs are stale.
        domains: Selected domain filters.
        statuses: Status filters applied only to the lookup table.
        recent_limit: Maximum rows for the lookup explorer.
        attention_limit: Maximum rows for the triage attention queue.

    Returns:
        Snapshot dictionary containing summary KPIs, triage rows, domain health
        inputs, activity charts, and lookup rows.
    """
    since = datetime.fromisoformat(since_iso)
    stale_after = datetime.fromisoformat(stale_after_iso)
    lake = DataLakeClient(read_only=True)
    try:
        available = pipeline_runs_available(lake)
        return {
            "available": available,
            "summary": load_status_summary(lake, since=since, domains=domains),
            "stale_runs": load_stale_running_runs(lake, older_than=stale_after, domains=domains),
            "attention_runs": load_attention_runs(
                lake,
                since=since,
                domains=domains,
                limit=attention_limit,
            ),
            "latest_runs": load_latest_runs_by_domain(lake, domains=domains),
            "latest_terminal_runs": load_latest_terminal_runs_by_domain(lake, domains=domains, since=since),
            "latest_attention_runs": load_latest_attention_runs_by_domain(lake, since=since, domains=domains),
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


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_run_page(run_id: str) -> dict[str, Any]:
    """Load run summary and investigation detail for one pipeline run.

    Args:
        run_id: Durable run identifier from ``pipeline.runs``.

    Returns:
        Dictionary with ``run`` and nested ``detail`` lists for units, landing,
        rejections, and dbt node results.
    """
    lake = DataLakeClient(read_only=True)
    try:
        return {
            "run": load_run_by_id(lake, run_id=run_id),
            "detail": {
                "unit_status": load_unit_status_breakdown(lake, run_id=run_id),
                "run_units": load_run_units(lake, run_id=run_id, limit=RUN_UNITS_LIMIT),
                "landing_objects": load_landing_objects(lake, run_id=run_id, limit=LANDING_OBJECTS_LIMIT),
                "rejections": load_rejections(lake, run_id=run_id, limit=REJECTIONS_LIMIT),
                "dbt_node_results": load_dbt_node_results(lake, run_id=run_id, limit=DBT_NODE_RESULTS_LIMIT),
            },
        }
    finally:
        lake.close()


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_unit_page(*, run_id: str, unit_id: str) -> dict[str, Any]:
    """Load parent run context and unit-scoped evidence rows.

    Args:
        run_id: Parent run identifier.
        unit_id: Durable work-unit identifier.

    Returns:
        Dictionary with ``run``, ``unit``, and unit-scoped ``detail`` lists.
    """
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
                    limit=LANDING_OBJECTS_LIMIT,
                ),
                "rejections": load_rejections(
                    lake,
                    run_id=run_id,
                    unit_id=unit_id,
                    limit=REJECTIONS_LIMIT,
                ),
            },
        }
    finally:
        lake.close()
