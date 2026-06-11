"""Read-only query helpers for the pipeline audit dashboard."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, cast

from core.clients.lake.sql import SqlTemplateContext

DEFAULT_DASHBOARD_DOMAINS = ("eod_price", "exchange", "exchange_schedule", "instrument", "fundamental", "dbt")
RUN_STATUSES = ("running", "completed", "partial", "failed", "skipped", "cancelled")
ATTENTION_STATUSES = ("failed", "partial")
HEALTHY_TERMINAL_STATUSES = ("completed", "partial", "skipped")
_SQL_DIR = Path(__file__).with_name("sql")


class LakeReader(Protocol):
    """Read-only lake methods used by dashboard queries."""

    def table_exists(self, schema: str, table: str) -> bool:
        """Return whether a lake table exists."""
        ...

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Run a SELECT statement and return rows."""
        ...

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Run a SELECT statement and return one row."""
        ...

    def query_file(
        self,
        sql_path: str | Path,
        params: Sequence[Any] | None = None,
        *,
        template_context: SqlTemplateContext | None = None,
    ) -> list[dict[str, Any]]:
        """Run a SELECT statement from a SQL file and return rows."""
        ...

    def query_one_file(
        self,
        sql_path: str | Path,
        params: Sequence[Any] | None = None,
        *,
        template_context: SqlTemplateContext | None = None,
    ) -> dict[str, Any] | None:
        """Run a SELECT statement from a SQL file and return one row."""
        ...


def pipeline_runs_available(lake: LakeReader) -> bool:
    """Return whether the dashboard's base audit table exists."""
    return lake.table_exists("pipeline", "runs")


def run_units_available(lake: LakeReader) -> bool:
    """Return whether the dashboard's work-unit table exists."""
    return lake.table_exists("pipeline", "run_units")


def landing_objects_available(lake: LakeReader) -> bool:
    """Return whether the dashboard's landing-object table exists."""
    return lake.table_exists("pipeline", "landing_objects")


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


def load_audit_evidence_summary(
    lake: LakeReader,
    *,
    since: datetime,
    domains: Sequence[str],
) -> dict[str, int]:
    """Load compact cross-table audit evidence counters for the overview page."""
    summary = _empty_evidence_summary()
    domain_values = _clean_values(domains)

    if lake.table_exists("pipeline", "landing_objects"):
        clauses = ["recorded_at >= ?"]
        params: list[Any] = [since]
        if domain_values:
            clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one_file(
            _SQL_DIR / "load_landing_evidence_summary.sql",
            params,
            template_context={"where_clauses": " AND ".join(clauses)},
        )
        if row:
            summary["landing_objects"] = _int_value(row.get("landing_objects"))
            summary["landing_bytes"] = _int_value(row.get("landing_bytes"))

    if lake.table_exists("pipeline", "rejections"):
        clauses = ["recorded_at >= ?"]
        params = [since]
        if domain_values:
            clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one_file(
            _SQL_DIR / "load_rejection_evidence_summary.sql",
            params,
            template_context={"where_clauses": " AND ".join(clauses)},
        )
        if row:
            summary["rejection_samples"] = _int_value(row.get("rejection_samples"))

    if lake.table_exists("pipeline", "ingestion_coverage"):
        clauses = ["recorded_at >= ?"]
        params = [since]
        if domain_values:
            clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one_file(
            _SQL_DIR / "load_coverage_evidence_summary.sql",
            params,
            template_context={"where_clauses": " AND ".join(clauses)},
        )
        if row:
            summary["coverage_records"] = _int_value(row.get("coverage_records"))

    if lake.table_exists("pipeline", "dbt_invocations") and pipeline_runs_available(lake):
        clauses = ["run.started_at >= ?"]
        params = [since]
        if domain_values:
            clauses.append(f"run.domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one_file(
            _SQL_DIR / "load_dbt_invocation_evidence_summary.sql",
            params,
            template_context={"where_clauses": " AND ".join(clauses)},
        )
        if row:
            summary["dbt_invocations"] = _int_value(row.get("dbt_invocations"))
            summary["dbt_attention_invocations"] = _int_value(row.get("dbt_attention_invocations"))

    if (
        lake.table_exists("pipeline", "dbt_invocations")
        and lake.table_exists("pipeline", "dbt_node_results")
        and pipeline_runs_available(lake)
    ):
        clauses = ["run.started_at >= ?"]
        params = [since]
        if domain_values:
            clauses.append(f"run.domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one_file(
            _SQL_DIR / "load_dbt_attention_node_evidence_summary.sql",
            params,
            template_context={"where_clauses": " AND ".join(clauses)},
        )
        if row:
            summary["dbt_attention_nodes"] = _int_value(row.get("dbt_attention_nodes"))

    return summary


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


def load_rejections(
    lake: LakeReader,
    *,
    run_id: str,
    unit_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Load sampled parser rejections linked to a selected run or work unit."""
    if not lake.table_exists("pipeline", "rejections"):
        return []
    clauses = ["run_id = ?"]
    params: list[Any] = [run_id]
    if unit_id:
        clauses.append("unit_id = ?")
        params.append(unit_id)
    params.append(_bounded_limit(limit, default=100, maximum=1000))
    return lake.query_file(
        _SQL_DIR / "load_rejections.sql",
        params,
        template_context={"where_clauses": " AND ".join(clauses)},
    )


def load_dbt_node_results(lake: LakeReader, *, run_id: str, limit: int = 300) -> list[dict[str, Any]]:
    """Load dbt model/test node results linked to a selected pipeline run."""
    if not lake.table_exists("pipeline", "dbt_invocations") or not lake.table_exists("pipeline", "dbt_node_results"):
        return []
    return lake.query_file(
        _SQL_DIR / "load_dbt_node_results.sql",
        [run_id, _bounded_limit(limit, default=300, maximum=2000)],
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


def _clean_values(values: Sequence[str]) -> list[str]:
    """Drop blank strings from dashboard filter inputs.

    Args:
        values: Raw string filter values from the UI.

    Returns:
        Non-empty trimmed strings.
    """
    return [str(value) for value in values if str(value).strip()]


def _placeholders(count: int) -> str:
    """Build a comma-separated SQL placeholder list.

    Args:
        count: Number of placeholders to generate.

    Returns:
        Placeholder string such as ``?, ?, ?``.
    """
    return ", ".join("?" for _ in range(count))


def _bounded_limit(value: int, *, default: int, maximum: int) -> int:
    """Clamp user-provided limits to safe dashboard bounds.

    Args:
        value: Requested row limit from the UI or caller.
        default: Fallback when ``value`` cannot be parsed as an integer.
        maximum: Hard upper bound enforced for lake queries.

    Returns:
        Integer limit between ``1`` and ``maximum``.
    """
    try:
        numeric = int(value)
    except TypeError, ValueError:
        return default
    return max(1, min(numeric, maximum))


def _empty_summary() -> dict[str, int]:
    """Return zeroed KPI counters for unmigrated or empty lakes.

    Returns:
        Empty run-summary counter mapping used by overview KPIs.
    """
    return {
        "total_runs": 0,
        "running_runs": 0,
        "completed_runs": 0,
        "partial_runs": 0,
        "failed_runs": 0,
        "attention_runs": 0,
        "units_failed": 0,
        "rows_written": 0,
        "rows_rejected": 0,
    }


def _empty_evidence_summary() -> dict[str, int]:
    """Return zeroed cross-table audit evidence counters."""
    return {
        "landing_objects": 0,
        "landing_bytes": 0,
        "rejection_samples": 0,
        "coverage_records": 0,
        "dbt_invocations": 0,
        "dbt_attention_invocations": 0,
        "dbt_attention_nodes": 0,
    }


def _int_value(value: object) -> int:
    """Coerce lake counter values to integers.

    Args:
        value: Raw scalar from a SQL aggregate.

    Returns:
        Integer value, defaulting to zero for ``None``.
    """
    if value is None:
        return 0
    return int(cast(Any, value))
