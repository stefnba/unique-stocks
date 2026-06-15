"""Check production-facing pipeline health from Prefect and lake audit state."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from core.lake import DataLakeClient
from core.operations.health_common import prefect_api_is_healthy
from core.prefect.events import emit_prefect_stale_runs_event
from core.utils.redaction import redact_sensitive_query_params

type LakeFactory = Callable[..., "OperationalHealthLake"]
type PrefectApiHealthCheck = Callable[[str], bool]
type StaleRunsEventEmitter = Callable[..., None]


class OperationalHealthLake(Protocol):
    """Lake interface needed by the operational health checks."""

    def table_exists(self, schema: str, table: str) -> bool:
        """Return whether a table exists."""
        ...

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Run a SELECT and return rows."""
        ...

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Run a SELECT and return one row."""
        ...

    def close(self) -> None:
        """Close the lake connection."""
        ...


@dataclass(frozen=True, slots=True)
class OperationalHealthConfig:
    """Configuration for one operational health evaluation."""

    prefect_api_url: str
    stale_running_hours: float
    recent_domains: Sequence[str]
    recent_hours: float
    lake_read_only: bool


@dataclass(frozen=True, slots=True)
class OperationalHealthResult:
    """Structured result of an operational health evaluation."""

    failures: list[str]

    @property
    def passed(self) -> bool:
        """Return whether all checks passed."""
        return not self.failures


def stale_running_runs(lake: OperationalHealthLake, *, older_than: datetime) -> list[dict[str, object]]:
    """Return pipeline runs stuck in running status older than the threshold."""
    if not lake.table_exists("pipeline", "runs"):
        return [{"flow_name": "pipeline.runs", "started_at": None, "status": "missing_table"}]
    return lake.query(
        """
        SELECT run_id, flow_name, domain, started_at
        FROM pipeline.runs
        WHERE status = 'running'
          AND started_at < ?
        ORDER BY started_at
        """,
        [older_than],
    )


def recent_domain_runs(
    lake: OperationalHealthLake,
    *,
    domains: Sequence[str],
    since: datetime,
) -> dict[str, dict[str, object] | None]:
    """Return the latest completed run for each required domain since the threshold."""
    latest: dict[str, dict[str, object] | None] = {}
    if not domains:
        return latest
    if not lake.table_exists("pipeline", "runs"):
        return {domain: None for domain in domains}

    for domain in domains:
        latest[domain] = lake.query_one(
            """
            SELECT run_id, flow_name, domain, status, completed_at
            FROM pipeline.runs
            WHERE domain = ?
              AND status = 'completed'
              AND completed_at >= ?
            ORDER BY completed_at DESC
            LIMIT 1
            """,
            [domain, since],
        )
    return latest


def run_operational_health(
    config: OperationalHealthConfig,
    *,
    lake_factory: LakeFactory = DataLakeClient,
    api_health_check: PrefectApiHealthCheck = prefect_api_is_healthy,
    stale_runs_event: StaleRunsEventEmitter = emit_prefect_stale_runs_event,
    now: datetime | None = None,
) -> OperationalHealthResult:
    """Evaluate Prefect API and lake audit health."""
    failures: list[str] = []

    if not config.prefect_api_url:
        failures.append("PREFECT_API_URL is not set")
    elif not api_health_check(config.prefect_api_url):
        failures.append("Prefect API health endpoint is not reachable")

    observed_at = now or datetime.now(UTC)
    lake: OperationalHealthLake | None = None
    try:
        lake = lake_factory(read_only=config.lake_read_only, ensure_database=False, schemas=())
        stale_runs = stale_running_runs(
            lake,
            older_than=observed_at - timedelta(hours=max(0.0, config.stale_running_hours)),
        )
        if stale_runs:
            failures.append(f"{len(stale_runs)} stale running pipeline run(s)")
            stale_runs_event(
                stale_runs=stale_runs,
                older_than_minutes=int(max(0.0, config.stale_running_hours) * 60),
            )

        latest_by_domain = recent_domain_runs(
            lake,
            domains=config.recent_domains,
            since=observed_at - timedelta(hours=max(0.0, config.recent_hours)),
        )
        missing_domains = [domain for domain, row in latest_by_domain.items() if row is None]
        if missing_domains:
            failures.append(f"missing recent completed run for domain(s): {', '.join(missing_domains)}")
    except Exception as exc:
        safe_error = redact_sensitive_query_params(str(exc))
        failures.append(f"lake audit health check failed ({type(exc).__name__}: {safe_error})")
    finally:
        if lake is not None:
            lake.close()

    return OperationalHealthResult(failures=failures)
