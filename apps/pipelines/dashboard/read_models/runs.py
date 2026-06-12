"""Run-level dashboard lake queries."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from dashboard.read_models.base import (
    _SQL_DIR,
    LakeReader,
    _bounded_limit,
    _clean_values,
    _empty_summary,
    _int_value,
    _placeholders,
    pipeline_runs_available,
)


def load_status_summary(
    lake: LakeReader,
    *,
    since: datetime,
    domains: Sequence[str],
) -> dict[str, int]:
    """Load run-level KPI counters for the selected window."""
    if not pipeline_runs_available(lake):
        return _empty_summary()

    clauses, params = _run_filters(since=since, domains=domains, statuses=())
    row = lake.query_one_file(
        _SQL_DIR / "load_status_summary.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )
    if row is None:
        return _empty_summary()
    return {key: _int_value(row.get(key)) for key in _empty_summary()}


def load_stale_running_runs(
    lake: LakeReader,
    *,
    older_than: datetime,
    domains: Sequence[str],
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Load runs that have been in ``running`` state past the stale threshold."""
    if not pipeline_runs_available(lake):
        return []

    clauses = ["status = 'running'", "started_at < ?"]
    params: list[Any] = [older_than]
    domain_values = _clean_values(domains)
    if domain_values:
        clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
        params.extend(domain_values)

    params.append(_bounded_limit(limit, default=100, maximum=500))
    return lake.query_file(
        _SQL_DIR / "load_stale_running_runs.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )


def load_latest_runs_by_domain(lake: LakeReader, *, domains: Sequence[str]) -> list[dict[str, Any]]:
    """Load the latest run for each selected domain."""
    if not pipeline_runs_available(lake):
        return []

    clauses = []
    params: list[Any] = []
    domain_values = _clean_values(domains)
    if domain_values:
        clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
        params.extend(domain_values)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    return lake.query_file(
        _SQL_DIR / "load_latest_runs_by_domain.sql",
        params,
        template_context={"where_sql": where_sql},
    )


def load_latest_terminal_runs_by_domain(
    lake: LakeReader,
    *,
    domains: Sequence[str],
    since: datetime,
) -> list[dict[str, Any]]:
    """Load latest healthy terminal runs by domain for freshness monitoring."""
    if not pipeline_runs_available(lake):
        return []

    domain_values = _clean_values(domains)
    clauses = ["status IN ('completed', 'partial', 'skipped')", "completed_at >= ?"]
    params: list[Any] = [since]
    if domain_values:
        clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
        params.extend(domain_values)

    return lake.query_file(
        _SQL_DIR / "load_latest_terminal_runs_by_domain.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )


def load_status_breakdown(
    lake: LakeReader,
    *,
    since: datetime,
    domains: Sequence[str],
) -> list[dict[str, Any]]:
    """Load run counts by status."""
    if not pipeline_runs_available(lake):
        return []

    clauses, params = _run_filters(since=since, domains=domains, statuses=())
    return lake.query_file(
        _SQL_DIR / "load_status_breakdown.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )


def load_domain_run_summary(
    lake: LakeReader,
    *,
    since: datetime,
    domains: Sequence[str],
) -> list[dict[str, Any]]:
    """Load per-domain run, unit, and row counters for the selected window."""
    if not pipeline_runs_available(lake):
        return []

    clauses, params = _run_filters(since=since, domains=domains, statuses=())
    return lake.query_file(
        _SQL_DIR / "load_domain_run_summary.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )


def load_daily_run_trend(
    lake: LakeReader,
    *,
    since: datetime,
    domains: Sequence[str],
) -> list[dict[str, Any]]:
    """Load daily run volume and failure trend by domain."""
    if not pipeline_runs_available(lake):
        return []

    clauses, params = _run_filters(since=since, domains=domains, statuses=())
    return lake.query_file(
        _SQL_DIR / "load_daily_run_trend.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )


def load_attention_runs(
    lake: LakeReader,
    *,
    since: datetime,
    domains: Sequence[str],
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Load failed and partial runs for triage, independent of explorer status filters.

    Args:
        lake: Read-only lake client.
        since: Start of the dashboard window.
        domains: Optional domain filters. An empty sequence disables domain filtering.
        limit: Maximum number of attention runs to return.

    Returns:
        Failed and partial run rows ordered by ``started_at`` descending.
    """
    if not pipeline_runs_available(lake):
        return []

    domain_values = _clean_values(domains)
    clauses = ["started_at >= ?", "status IN ('failed', 'partial')"]
    params: list[Any] = [since]
    if domain_values:
        clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
        params.extend(domain_values)

    params.append(_bounded_limit(limit, default=200, maximum=500))
    return lake.query_file(
        _SQL_DIR / "load_attention_runs.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )


def load_latest_attention_runs_by_domain(
    lake: LakeReader,
    *,
    since: datetime,
    domains: Sequence[str],
) -> list[dict[str, Any]]:
    """Load the most recent failed or partial run per domain inside the window.

    Args:
        lake: Read-only lake client.
        since: Start of the dashboard window.
        domains: Optional domain filters. An empty sequence disables domain filtering.

    Returns:
        One attention run row per domain, ordered by domain name.
    """
    if not pipeline_runs_available(lake):
        return []

    domain_values = _clean_values(domains)
    clauses = ["started_at >= ?", "status IN ('failed', 'partial')"]
    params: list[Any] = [since]
    if domain_values:
        clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
        params.extend(domain_values)

    return lake.query_file(
        _SQL_DIR / "load_latest_attention_runs_by_domain.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )


def load_recent_runs(
    lake: LakeReader,
    *,
    since: datetime,
    domains: Sequence[str],
    statuses: Sequence[str],
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Load recent runs for the main dashboard table."""
    if not pipeline_runs_available(lake):
        return []

    clauses, params = _run_filters(since=since, domains=domains, statuses=statuses)
    params.append(_bounded_limit(limit, default=200, maximum=1000))
    return lake.query_file(
        _SQL_DIR / "load_recent_runs.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )


def load_run_by_id(lake: LakeReader, *, run_id: str) -> dict[str, Any] | None:
    """Load one run by durable run id."""
    if not pipeline_runs_available(lake):
        return None
    return lake.query_one_file(
        _SQL_DIR / "load_run_by_id.sql",
        [run_id],
    )


def _run_filters(
    *,
    since: datetime,
    domains: Sequence[str],
    statuses: Sequence[str],
) -> tuple[list[str], list[Any]]:
    """Build shared ``WHERE`` clauses for run-level dashboard queries.

    Args:
        since: Lower bound for ``started_at``.
        domains: Optional domain filters.
        statuses: Optional status filters. An empty sequence disables status filtering.

    Returns:
        Tuple of SQL clause strings and bound parameter values.
    """
    clauses = ["started_at >= ?"]
    params: list[Any] = [since]

    domain_values = _clean_values(domains)
    if domain_values:
        clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
        params.extend(domain_values)

    status_values = _clean_values(statuses)
    if status_values:
        clauses.append(f"status IN ({_placeholders(len(status_values))})")
        params.extend(status_values)

    return clauses, params
