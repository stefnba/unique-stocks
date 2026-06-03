"""Read-only query helpers for the pipeline audit dashboard."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any, Protocol, cast

DEFAULT_DASHBOARD_DOMAINS = ("eod_price", "exchange", "exchange_schedule", "instrument", "fundamental", "dbt")
RUN_STATUSES = ("running", "completed", "partial", "failed", "skipped", "cancelled")
ATTENTION_STATUSES = ("failed", "partial")
HEALTHY_TERMINAL_STATUSES = ("completed", "partial", "skipped")


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
    row = lake.query_one(
        f"""
        SELECT
            COUNT(*) AS total_runs,
            COUNT(*) FILTER (WHERE status = 'running') AS running_runs,
            COUNT(*) FILTER (WHERE status = 'completed') AS completed_runs,
            COUNT(*) FILTER (WHERE status = 'partial') AS partial_runs,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed_runs,
            COUNT(*) FILTER (WHERE status IN ('failed', 'partial')) AS attention_runs,
            COALESCE(SUM(units_failed), 0) AS units_failed,
            COALESCE(SUM(rows_written), 0) AS rows_written,
            COALESCE(SUM(rows_rejected), 0) AS rows_rejected
        FROM pipeline.runs
        WHERE {" AND ".join(clauses)}
        """,
        params,
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
    return lake.query(
        f"""
        SELECT
            run_id,
            prefect_flow_run_id,
            flow_name,
            domain,
            run_kind,
            provider,
            status,
            started_at,
            completed_at,
            date_diff('second', started_at, CURRENT_TIMESTAMP) AS running_seconds,
            date_diff('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
            units_total,
            units_failed,
            rows_written,
            rows_rejected,
            error_class,
            error_message
        FROM pipeline.runs
        WHERE {" AND ".join(clauses)}
        ORDER BY started_at
        LIMIT ?
        """,
        params,
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

    return lake.query(
        f"""
        WITH ranked AS (
            SELECT
                run_id,
                prefect_flow_run_id,
                flow_name,
                domain,
                run_kind,
                provider,
                status,
                started_at,
                completed_at,
                date_diff('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
                units_total,
                units_failed,
                rows_written,
                rows_rejected,
                error_class,
                error_message,
                ROW_NUMBER() OVER (PARTITION BY domain ORDER BY started_at DESC) AS row_number
            FROM pipeline.runs
            {where_sql}
        )
        SELECT * EXCLUDE (row_number)
        FROM ranked
        WHERE row_number = 1
        ORDER BY domain
        """,
        params,
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

    return lake.query(
        f"""
        WITH ranked AS (
            SELECT
                run_id,
                flow_name,
                domain,
                run_kind,
                status,
                completed_at,
                rows_written,
                rows_rejected,
                ROW_NUMBER() OVER (PARTITION BY domain ORDER BY completed_at DESC) AS row_number
            FROM pipeline.runs
            WHERE {" AND ".join(clauses)}
        )
        SELECT * EXCLUDE (row_number)
        FROM ranked
        WHERE row_number = 1
        ORDER BY domain
        """,
        params,
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
    return lake.query(
        f"""
        SELECT status, COUNT(*) AS runs
        FROM pipeline.runs
        WHERE {" AND ".join(clauses)}
        GROUP BY status
        ORDER BY runs DESC, status
        """,
        params,
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
    return lake.query(
        f"""
        SELECT
            domain,
            COUNT(*) AS runs,
            COUNT(*) FILTER (WHERE status = 'running') AS running_runs,
            COUNT(*) FILTER (WHERE status = 'completed') AS completed_runs,
            COUNT(*) FILTER (WHERE status IN ('failed', 'partial')) AS attention_runs,
            COALESCE(SUM(units_failed), 0) AS units_failed,
            COALESCE(SUM(rows_written), 0) AS rows_written,
            COALESCE(SUM(rows_rejected), 0) AS rows_rejected,
            MAX(started_at) AS latest_started_at
        FROM pipeline.runs
        WHERE {" AND ".join(clauses)}
        GROUP BY domain
        ORDER BY attention_runs DESC, running_runs DESC, rows_rejected DESC, domain
        """,
        params,
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
    return lake.query(
        f"""
        SELECT
            CAST(started_at AS DATE) AS run_date,
            domain,
            COUNT(*) AS runs,
            COUNT(*) FILTER (WHERE status IN ('failed', 'partial')) AS attention_runs,
            COALESCE(SUM(rows_written), 0) AS rows_written,
            COALESCE(SUM(rows_rejected), 0) AS rows_rejected
        FROM pipeline.runs
        WHERE {" AND ".join(clauses)}
        GROUP BY run_date, domain
        ORDER BY run_date, domain
        """,
        params,
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
        row = lake.query_one(
            f"""
            SELECT
                COUNT(*) AS landing_objects,
                COALESCE(SUM(byte_count), 0) AS landing_bytes
            FROM pipeline.landing_objects
            WHERE {" AND ".join(clauses)}
            """,
            params,
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
        row = lake.query_one(
            f"""
            SELECT COUNT(*) AS rejection_samples
            FROM pipeline.rejections
            WHERE {" AND ".join(clauses)}
            """,
            params,
        )
        if row:
            summary["rejection_samples"] = _int_value(row.get("rejection_samples"))

    if lake.table_exists("pipeline", "ingestion_coverage"):
        clauses = ["recorded_at >= ?"]
        params = [since]
        if domain_values:
            clauses.append(f"domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one(
            f"""
            SELECT COUNT(*) AS coverage_records
            FROM pipeline.ingestion_coverage
            WHERE {" AND ".join(clauses)}
            """,
            params,
        )
        if row:
            summary["coverage_records"] = _int_value(row.get("coverage_records"))

    if lake.table_exists("pipeline", "dbt_invocations") and pipeline_runs_available(lake):
        clauses = ["run.started_at >= ?"]
        params = [since]
        if domain_values:
            clauses.append(f"run.domain IN ({_placeholders(len(domain_values))})")
            params.extend(domain_values)
        row = lake.query_one(
            f"""
            SELECT
                COUNT(*) AS dbt_invocations,
                COUNT(*) FILTER (
                    WHERE invocation.status NOT IN ('completed', 'success', 'pass')
                ) AS dbt_attention_invocations
            FROM pipeline.dbt_invocations AS invocation
            INNER JOIN pipeline.runs AS run
                ON invocation.run_id = run.run_id
            WHERE {" AND ".join(clauses)}
            """,
            params,
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
        row = lake.query_one(
            f"""
            SELECT COUNT(*) AS dbt_attention_nodes
            FROM pipeline.dbt_invocations AS invocation
            INNER JOIN pipeline.dbt_node_results AS node
                ON invocation.dbt_run_id = node.dbt_run_id
            INNER JOIN pipeline.runs AS run
                ON invocation.run_id = run.run_id
            WHERE {" AND ".join(clauses)}
              AND node.status IN ('error', 'fail', 'warn')
            """,
            params,
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
    return lake.query(
        f"""
        SELECT
            run_id,
            parent_run_id,
            prefect_flow_run_id,
            flow_name,
            domain,
            run_kind,
            provider,
            status,
            started_at,
            completed_at,
            date_diff('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
            units_total,
            units_failed,
            rows_written,
            rows_rejected,
            error_class,
            error_message
        FROM pipeline.runs
        WHERE {" AND ".join(clauses)}
        ORDER BY started_at DESC
        LIMIT ?
        """,
        params,
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

    return lake.query(
        f"""
        WITH ranked AS (
            SELECT
                run_id,
                flow_name,
                domain,
                status,
                started_at,
                completed_at,
                units_failed,
                error_class,
                error_message,
                ROW_NUMBER() OVER (PARTITION BY domain ORDER BY started_at DESC) AS row_number
            FROM pipeline.runs
            WHERE {" AND ".join(clauses)}
        )
        SELECT * EXCLUDE (row_number)
        FROM ranked
        WHERE row_number = 1
        ORDER BY domain
        """,
        params,
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
    return lake.query(
        f"""
        SELECT
            run_id,
            parent_run_id,
            prefect_flow_run_id,
            flow_name,
            domain,
            run_kind,
            provider,
            environment,
            code_version,
            parameters_json,
            target_window_start,
            target_window_end,
            status,
            started_at,
            completed_at,
            date_diff('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
            units_total,
            units_succeeded,
            units_failed,
            units_skipped,
            rows_raw,
            rows_valid,
            rows_rejected,
            rows_written,
            summary_json,
            error_class,
            error_message
        FROM pipeline.runs
        WHERE {" AND ".join(clauses)}
        ORDER BY started_at DESC
        LIMIT ?
        """,
        params,
    )


def load_run_by_id(lake: LakeReader, *, run_id: str) -> dict[str, Any] | None:
    """Load one run by durable run id."""
    if not pipeline_runs_available(lake):
        return None
    return lake.query_one(
        """
        SELECT
            run_id,
            parent_run_id,
            prefect_flow_run_id,
            flow_name,
            domain,
            run_kind,
            provider,
            environment,
            code_version,
            parameters_json,
            target_window_start,
            target_window_end,
            status,
            started_at,
            completed_at,
            date_diff('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
            units_total,
            units_succeeded,
            units_failed,
            units_skipped,
            rows_raw,
            rows_valid,
            rows_rejected,
            rows_written,
            summary_json,
            error_class,
            error_message
        FROM pipeline.runs
        WHERE run_id = ?
        LIMIT 1
        """,
        [run_id],
    )


def load_unit_status_breakdown(lake: LakeReader, *, run_id: str) -> list[dict[str, Any]]:
    """Load work-unit counts by status for a run."""
    if not lake.table_exists("pipeline", "run_units"):
        return []
    return lake.query(
        """
        SELECT status, COUNT(*) AS units
        FROM pipeline.run_units
        WHERE run_id = ?
        GROUP BY status
        ORDER BY units DESC, status
        """,
        [run_id],
    )


def load_run_unit_by_id(lake: LakeReader, *, run_id: str, unit_id: str) -> dict[str, Any] | None:
    """Load one work unit by durable unit id."""
    if not lake.table_exists("pipeline", "run_units"):
        return None
    return lake.query_one(
        """
        SELECT
            unit_id,
            run_id,
            domain,
            provider,
            unit_type,
            unit_key_hash,
            unit_key_json,
            status,
            reason,
            source_uri,
            rows_raw,
            rows_valid,
            rows_rejected,
            rows_written,
            started_at,
            completed_at,
            date_diff('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
            error_class,
            error_message
        FROM pipeline.run_units
        WHERE run_id = ? AND unit_id = ?
        LIMIT 1
        """,
        [run_id, unit_id],
    )


def load_run_units(lake: LakeReader, *, run_id: str, limit: int = 500) -> list[dict[str, Any]]:
    """Load work-unit rows for a selected run."""
    if not lake.table_exists("pipeline", "run_units"):
        return []
    return lake.query(
        """
        SELECT
            unit_id,
            unit_type,
            unit_key_hash,
            unit_key_json,
            status,
            reason,
            provider,
            source_uri,
            rows_raw,
            rows_valid,
            rows_rejected,
            rows_written,
            started_at,
            completed_at,
            date_diff('second', started_at, COALESCE(completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
            error_class,
            error_message
        FROM pipeline.run_units
        WHERE run_id = ?
        ORDER BY
            CASE status
                WHEN 'failed' THEN 1
                WHEN 'unsupported' THEN 2
                WHEN 'skipped' THEN 3
                ELSE 4
            END,
            completed_at DESC NULLS LAST,
            started_at DESC NULLS LAST
        LIMIT ?
        """,
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
    return lake.query(
        f"""
        SELECT
            unit.unit_id,
            unit.run_id,
            {flow_sql} AS flow_name,
            {run_kind_sql} AS run_kind,
            {domain_sql} AS domain,
            {provider_sql} AS provider,
            unit.unit_type,
            unit.unit_key_hash,
            unit.unit_key_json,
            unit.status,
            unit.reason,
            unit.source_uri,
            unit.rows_raw,
            unit.rows_valid,
            unit.rows_rejected,
            unit.rows_written,
            unit.started_at,
            unit.completed_at,
            date_diff('second', unit.started_at, COALESCE(unit.completed_at, CURRENT_TIMESTAMP)) AS duration_seconds,
            unit.error_class,
            unit.error_message
        FROM pipeline.run_units AS unit
        {join_sql}
        WHERE {" AND ".join(clauses)}
        ORDER BY
            CASE unit.status
                WHEN 'failed' THEN 1
                WHEN 'unsupported' THEN 2
                WHEN 'skipped' THEN 3
                WHEN 'running' THEN 4
                ELSE 5
            END,
            unit.completed_at DESC NULLS LAST,
            unit.started_at DESC NULLS LAST
        LIMIT ?
        """,
        params,
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
    return lake.query(
        f"""
        SELECT
            landing_id,
            run_id,
            unit_id,
            domain,
            dataset,
            provider,
            source_uri,
            partition_json,
            rows_raw,
            byte_count,
            content_hash,
            recorded_at
        FROM pipeline.landing_objects
        WHERE {" AND ".join(clauses)}
        ORDER BY recorded_at DESC
        LIMIT ?
        """,
        params,
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
    return lake.query(
        f"""
        SELECT
            landing.landing_id,
            landing.run_id,
            landing.unit_id,
            {run_flow_sql} AS flow_name,
            {domain_sql} AS domain,
            {provider_sql} AS provider,
            landing.dataset,
            landing.source_uri,
            landing.partition_json,
            landing.rows_raw,
            landing.byte_count,
            landing.content_hash,
            landing.recorded_at
        FROM pipeline.landing_objects AS landing
        {run_join_sql}
        {unit_join_sql}
        WHERE {" AND ".join(clauses)}
        ORDER BY landing.recorded_at DESC
        LIMIT ?
        """,
        params,
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

    return lake.query_one(
        f"""
        SELECT
            landing.landing_id,
            landing.run_id,
            landing.unit_id,
            {run_flow_sql} AS flow_name,
            {run_status_sql} AS run_status,
            {unit_status_sql} AS unit_status,
            {domain_sql} AS domain,
            {provider_sql} AS provider,
            landing.dataset,
            landing.source_uri,
            landing.partition_json,
            landing.rows_raw,
            landing.byte_count,
            landing.content_hash,
            landing.recorded_at
        FROM pipeline.landing_objects AS landing
        {run_join_sql}
        {unit_join_sql}
        WHERE landing.landing_id = ?
        LIMIT 1
        """,
        [landing_id],
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
    return lake.query(
        f"""
        SELECT
            rejection_id,
            unit_id,
            domain,
            entity_key_json,
            source_uri,
            reason,
            error_class,
            error_message,
            raw_sample_json,
            recorded_at
        FROM pipeline.rejections
        WHERE {" AND ".join(clauses)}
        ORDER BY recorded_at DESC
        LIMIT ?
        """,
        params,
    )


def load_dbt_node_results(lake: LakeReader, *, run_id: str, limit: int = 300) -> list[dict[str, Any]]:
    """Load dbt model/test node results linked to a selected pipeline run."""
    if not lake.table_exists("pipeline", "dbt_invocations") or not lake.table_exists("pipeline", "dbt_node_results"):
        return []
    return lake.query(
        """
        SELECT
            invocation.dbt_run_id,
            invocation.command,
            invocation.target,
            invocation.return_code,
            invocation.elapsed_seconds AS invocation_elapsed_seconds,
            node.unique_id,
            node.resource_type,
            node.status,
            node.execution_time,
            node.failures,
            node.rows_affected,
            node.relation_name,
            node.message
        FROM pipeline.dbt_invocations AS invocation
        INNER JOIN pipeline.dbt_node_results AS node
            ON invocation.dbt_run_id = node.dbt_run_id
        WHERE invocation.run_id = ?
        ORDER BY
            CASE node.status
                WHEN 'error' THEN 1
                WHEN 'fail' THEN 2
                WHEN 'warn' THEN 3
                ELSE 4
            END,
            node.execution_time DESC NULLS LAST,
            node.unique_id
        LIMIT ?
        """,
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
