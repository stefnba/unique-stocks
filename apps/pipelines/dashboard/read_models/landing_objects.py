"""Landing-object dashboard lake queries."""

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
    landing_objects_available,
    pipeline_runs_available,
    run_units_available,
)


def load_landing_objects(
    lake: LakeReader,
    *,
    run_id: str,
    unit_id: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Load landing objects linked to a selected run or work unit."""
    if not lake.table_exists("pipeline", "landing_objects"):
        return []
    clauses = ["run_id = ?"]
    params: list[Any] = [run_id]
    if unit_id:
        clauses.append("unit_id = ?")
        params.append(unit_id)
    params.append(_bounded_limit(limit, default=200, maximum=1000))
    return lake.query_file(
        _SQL_DIR / "load_landing_objects.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )


def load_recent_landing_objects(
    lake: LakeReader,
    *,
    since: datetime,
    domains: Sequence[str],
    run_id: str | None = None,
    unit_id: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Load recent landing objects for the landing-object overview page."""
    if not landing_objects_available(lake):
        return []

    runs_available = pipeline_runs_available(lake)
    units_available = run_units_available(lake)
    run_join_sql = "LEFT JOIN pipeline.runs AS run ON landing.run_id = run.run_id" if runs_available else ""
    unit_join_sql = "LEFT JOIN pipeline.run_units AS unit ON landing.unit_id = unit.unit_id" if units_available else ""
    run_flow_sql = "run.flow_name" if runs_available else "CAST(NULL AS VARCHAR)"
    domain_inputs = ["landing.domain"]
    provider_inputs = ["landing.provider"]
    if units_available:
        domain_inputs.append("unit.domain")
        provider_inputs.append("unit.provider")
    if runs_available:
        domain_inputs.append("run.domain")
        provider_inputs.append("run.provider")
    domain_sql = f"COALESCE({', '.join(domain_inputs)})"
    provider_sql = f"COALESCE({', '.join(provider_inputs)})"

    clauses = ["landing.recorded_at >= ?"]
    params: list[Any] = [since]
    domain_values = _clean_values(domains)
    if domain_values:
        clauses.append(f"{domain_sql} IN ({_placeholders(len(domain_values))})")
        params.extend(domain_values)
    if run_id:
        clauses.append("landing.run_id = ?")
        params.append(run_id)
    if unit_id:
        clauses.append("landing.unit_id = ?")
        params.append(unit_id)

    params.append(_bounded_limit(limit, default=200, maximum=1000))
    return lake.query_file(
        _SQL_DIR / "load_recent_landing_objects.sql",
        params,
        template_context={
            "run_flow_sql": run_flow_sql,
            "domain_sql": domain_sql,
            "provider_sql": provider_sql,
            "run_join_sql": run_join_sql,
            "unit_join_sql": unit_join_sql,
            "where_clauses": " AND ".join(clauses),
        },
    )


def load_landing_object_by_id(lake: LakeReader, *, landing_id: str) -> dict[str, Any] | None:
    """Load one landing object by durable landing id."""
    if not landing_objects_available(lake):
        return None

    runs_available = pipeline_runs_available(lake)
    units_available = run_units_available(lake)
    run_join_sql = "LEFT JOIN pipeline.runs AS run ON landing.run_id = run.run_id" if runs_available else ""
    unit_join_sql = "LEFT JOIN pipeline.run_units AS unit ON landing.unit_id = unit.unit_id" if units_available else ""
    run_flow_sql = "run.flow_name" if runs_available else "CAST(NULL AS VARCHAR)"
    run_status_sql = "run.status" if runs_available else "CAST(NULL AS VARCHAR)"
    unit_status_sql = "unit.status" if units_available else "CAST(NULL AS VARCHAR)"
    domain_inputs = ["landing.domain"]
    provider_inputs = ["landing.provider"]
    if units_available:
        domain_inputs.append("unit.domain")
        provider_inputs.append("unit.provider")
    if runs_available:
        domain_inputs.append("run.domain")
        provider_inputs.append("run.provider")
    domain_sql = f"COALESCE({', '.join(domain_inputs)})"
    provider_sql = f"COALESCE({', '.join(provider_inputs)})"

    return lake.query_one_file(
        _SQL_DIR / "load_landing_object_by_id.sql",
        [landing_id],
        template_context={
            "run_flow_sql": run_flow_sql,
            "run_status_sql": run_status_sql,
            "unit_status_sql": unit_status_sql,
            "domain_sql": domain_sql,
            "provider_sql": provider_sql,
            "run_join_sql": run_join_sql,
            "unit_join_sql": unit_join_sql,
        },
    )
