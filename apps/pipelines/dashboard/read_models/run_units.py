"""Run-unit dashboard lake queries."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from dashboard.read_models.base import (
    _SQL_DIR,
    LakeReader,
    _bounded_limit,
    _clean_values,
    _placeholders,
    pipeline_runs_available,
    run_units_available,
)


def load_unit_status_breakdown(lake: LakeReader, *, run_id: str) -> list[dict[str, Any]]:
    """Load work-unit counts by status for a run."""
    if not lake.table_exists("pipeline", "run_units"):
        return []
    return lake.query_file(
        _SQL_DIR / "load_unit_status_breakdown.sql",
        [run_id],
    )


def load_run_unit_by_id(lake: LakeReader, *, run_id: str, unit_id: str) -> dict[str, Any] | None:
    """Load one work unit by durable unit id."""
    if not lake.table_exists("pipeline", "run_units"):
        return None
    return lake.query_one_file(
        _SQL_DIR / "load_run_unit_by_id.sql",
        [run_id, unit_id],
    )


def load_run_units(lake: LakeReader, *, run_id: str, limit: int = 500) -> list[dict[str, Any]]:
    """Load work-unit rows for a selected run."""
    if not lake.table_exists("pipeline", "run_units"):
        return []
    return lake.query_file(
        _SQL_DIR / "load_run_units.sql",
        [run_id, _bounded_limit(limit, default=500, maximum=2000)],
    )


def load_recent_run_units(
    lake: LakeReader,
    *,
    since: datetime,
    domains: Sequence[str],
    statuses: Sequence[str],
    run_id: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Load recent work units for the run-unit overview page."""
    if not run_units_available(lake):
        return []

    runs_available = pipeline_runs_available(lake)
    join_sql = "LEFT JOIN pipeline.runs AS run ON unit.run_id = run.run_id" if runs_available else ""
    flow_sql = "run.flow_name" if runs_available else "CAST(NULL AS VARCHAR)"
    run_kind_sql = "run.run_kind" if runs_available else "CAST(NULL AS VARCHAR)"
    domain_sql = "COALESCE(unit.domain, run.domain)" if runs_available else "unit.domain"
    provider_sql = "COALESCE(unit.provider, run.provider)" if runs_available else "unit.provider"
    window_sql = (
        "COALESCE(unit.started_at, unit.completed_at, run.started_at)"
        if runs_available
        else "COALESCE(unit.started_at, unit.completed_at)"
    )

    clauses = [f"{window_sql} >= ?"]
    params: list[Any] = [since]
    domain_values = _clean_values(domains)
    if domain_values:
        clauses.append(f"{domain_sql} IN ({_placeholders(len(domain_values))})")
        params.extend(domain_values)
    status_values = _clean_values(statuses)
    if status_values:
        clauses.append(f"unit.status IN ({_placeholders(len(status_values))})")
        params.extend(status_values)
    if run_id:
        clauses.append("unit.run_id = ?")
        params.append(run_id)

    params.append(_bounded_limit(limit, default=200, maximum=1000))
    return lake.query_file(
        _SQL_DIR / "load_recent_run_units.sql",
        params,
        template_context={
            "flow_sql": flow_sql,
            "run_kind_sql": run_kind_sql,
            "domain_sql": domain_sql,
            "provider_sql": provider_sql,
            "join_sql": join_sql,
            "where_clauses": " AND ".join(clauses),
        },
    )
