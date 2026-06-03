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
            started_at,
            date_diff('second', started_at, CURRENT_TIMESTAMP) AS running_seconds,
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


def load_landing_objects(lake: LakeReader, *, run_id: str, limit: int = 200) -> list[dict[str, Any]]:
    """Load landing objects linked to a selected run."""
    if not lake.table_exists("pipeline", "landing_objects"):
        return []
    return lake.query(
        """
        SELECT
            landing_id,
            unit_id,
            dataset,
            provider,
            source_uri,
            partition_json,
            rows_raw,
            byte_count,
            content_hash,
            recorded_at
        FROM pipeline.landing_objects
        WHERE run_id = ?
        ORDER BY recorded_at DESC
        LIMIT ?
        """,
        [run_id, _bounded_limit(limit, default=200, maximum=1000)],
    )


def load_rejections(lake: LakeReader, *, run_id: str, limit: int = 100) -> list[dict[str, Any]]:
    """Load sampled parser rejections linked to a selected run."""
    if not lake.table_exists("pipeline", "rejections"):
        return []
    return lake.query(
        """
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
        WHERE run_id = ?
        ORDER BY recorded_at DESC
        LIMIT ?
        """,
        [run_id, _bounded_limit(limit, default=100, maximum=1000)],
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
    return [str(value) for value in values if str(value).strip()]


def _placeholders(count: int) -> str:
    return ", ".join("?" for _ in range(count))


def _bounded_limit(value: int, *, default: int, maximum: int) -> int:
    try:
        numeric = int(value)
    except TypeError, ValueError:
        return default
    return max(1, min(numeric, maximum))


def _empty_summary() -> dict[str, int]:
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


def _int_value(value: object) -> int:
    if value is None:
        return 0
    return int(cast(Any, value))
