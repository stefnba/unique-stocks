"""Cached lake loaders for the pipeline audit dashboard."""

from collections.abc import Iterator
from contextlib import contextmanager
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
from dashboard.read_models.queries import (
    landing_objects_available,
    load_attention_runs,
    load_audit_evidence_summary,
    load_daily_run_trend,
    load_dbt_node_results,
    load_domain_run_summary,
    load_landing_object_by_id,
    load_landing_objects,
    load_latest_attention_runs_by_domain,
    load_latest_runs_by_domain,
    load_latest_terminal_runs_by_domain,
    load_recent_landing_objects,
    load_recent_run_units,
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
    run_units_available,
)


@contextmanager
def readonly_lake() -> Iterator[DataLakeClient]:
    """Yield a read-only lake client and close it after use."""
    lake = DataLakeClient(read_only=True)
    try:
        yield lake
    finally:
        lake.close()


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_snapshot(
    *,
    since_iso: str,
    stale_after_iso: str,
    domains: tuple[str, ...],
    attention_limit: int,
    include_triage: bool = True,
) -> dict[str, Any]:
    """Load lake data for overview or domain monitoring pages.

    Args:
        since_iso: ISO timestamp for the start of the dashboard window.
        stale_after_iso: ISO timestamp before which ``running`` runs are stale.
        domains: Selected domain filters.
        attention_limit: Maximum rows for the triage attention queue.
        include_triage: When false, skip stale and attention run lists (domains page).

    Returns:
        Snapshot dictionary containing summary KPIs, optional triage rows, domain health
        inputs, activity charts, and evidence counters.
    """
    since = datetime.fromisoformat(since_iso)
    stale_after = datetime.fromisoformat(stale_after_iso)
    with readonly_lake() as lake:
        available = pipeline_runs_available(lake)
        stale_runs: list[dict[str, Any]] = []
        attention_runs: list[dict[str, Any]] = []
        if include_triage:
            stale_runs = load_stale_running_runs(lake, older_than=stale_after, domains=domains)
            attention_runs = load_attention_runs(
                lake,
                since=since,
                domains=domains,
                limit=attention_limit,
            )
        return {
            "available": available,
            "summary": load_status_summary(lake, since=since, domains=domains),
            "evidence_summary": load_audit_evidence_summary(lake, since=since, domains=domains),
            "stale_runs": stale_runs,
            "attention_runs": attention_runs,
            "latest_runs": load_latest_runs_by_domain(lake, domains=domains),
            "latest_terminal_runs": load_latest_terminal_runs_by_domain(lake, domains=domains, since=since),
            "latest_attention_runs": load_latest_attention_runs_by_domain(lake, since=since, domains=domains),
            "domain_summary": load_domain_run_summary(lake, since=since, domains=domains),
            "status_breakdown": load_status_breakdown(lake, since=since, domains=domains),
            "daily_trend": load_daily_run_trend(lake, since=since, domains=domains),
        }


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_runs_page(
    *,
    since_iso: str,
    domains: tuple[str, ...],
    statuses: tuple[str, ...],
    recent_limit: int,
) -> dict[str, Any]:
    """Load run overview data for selected filters.

    Args:
        since_iso: ISO timestamp for the start of the run browser window.
        domains: Selected domain filters.
        statuses: Selected status filters.
        recent_limit: Maximum rows to fetch.

    Returns:
        Snapshot dictionary containing availability and matching run rows.
    """
    since = datetime.fromisoformat(since_iso)
    with readonly_lake() as lake:
        available = pipeline_runs_available(lake)
        return {
            "available": available,
            "recent_runs": load_recent_runs(
                lake,
                since=since,
                domains=domains,
                statuses=statuses,
                limit=recent_limit,
            ),
        }


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_run_units_overview_page(
    *,
    since_iso: str,
    domains: tuple[str, ...],
    statuses: tuple[str, ...],
    run_id: str | None,
    recent_limit: int,
) -> dict[str, Any]:
    """Load run-unit overview data for selected filters.

    Args:
        since_iso: ISO timestamp for the start of the browser window.
        domains: Selected domain filters.
        statuses: Selected unit status filters.
        run_id: Optional parent run identifier.
        recent_limit: Maximum rows to fetch.

    Returns:
        Snapshot dictionary containing availability and matching work-unit rows.
    """
    since = datetime.fromisoformat(since_iso)
    with readonly_lake() as lake:
        available = run_units_available(lake)
        return {
            "available": available,
            "recent_units": load_recent_run_units(
                lake,
                since=since,
                domains=domains,
                statuses=statuses,
                run_id=run_id,
                limit=recent_limit,
            ),
        }


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_landing_objects_overview_page(
    *,
    since_iso: str,
    domains: tuple[str, ...],
    run_id: str | None,
    unit_id: str | None,
    recent_limit: int,
) -> dict[str, Any]:
    """Load landing-object overview data for selected filters.

    Args:
        since_iso: ISO timestamp for the start of the browser window.
        domains: Selected domain filters.
        run_id: Optional parent run identifier.
        unit_id: Optional work-unit identifier.
        recent_limit: Maximum rows to fetch.

    Returns:
        Snapshot dictionary containing availability and matching landing objects.
    """
    since = datetime.fromisoformat(since_iso)
    with readonly_lake() as lake:
        available = landing_objects_available(lake)
        return {
            "available": available,
            "recent_landing_objects": load_recent_landing_objects(
                lake,
                since=since,
                domains=domains,
                run_id=run_id,
                unit_id=unit_id,
                limit=recent_limit,
            ),
        }


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_run_page(run_id: str) -> dict[str, Any]:
    """Load run summary and investigation detail for one pipeline run.

    Args:
        run_id: Durable run identifier from ``pipeline.runs``.

    Returns:
        Dictionary with ``run`` and nested ``detail`` lists for units, landing,
        rejections, and dbt node results.
    """
    with readonly_lake() as lake:
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


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_unit_page(*, run_id: str, unit_id: str) -> dict[str, Any]:
    """Load parent run context and unit-scoped evidence rows.

    Args:
        run_id: Parent run identifier.
        unit_id: Durable work-unit identifier.

    Returns:
        Dictionary with ``run``, ``unit``, and unit-scoped ``detail`` lists.
    """
    with readonly_lake() as lake:
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


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_landing_object_page(landing_id: str) -> dict[str, Any]:
    """Load one landing object and nearby context.

    Args:
        landing_id: Durable landing-object identifier.

    Returns:
        Dictionary with the landing object row, parent run, and parent work unit
        when those related rows are available.
    """
    with readonly_lake() as lake:
        landing_object = load_landing_object_by_id(lake, landing_id=landing_id)
        run = None
        unit = None
        if landing_object is not None:
            run_id = landing_object.get("run_id")
            unit_id = landing_object.get("unit_id")
            if run_id:
                run = load_run_by_id(lake, run_id=str(run_id))
            if run_id and unit_id:
                unit = load_run_unit_by_id(lake, run_id=str(run_id), unit_id=str(unit_id))
        return {
            "available": landing_objects_available(lake),
            "landing_object": landing_object,
            "run": run,
            "unit": unit,
        }
