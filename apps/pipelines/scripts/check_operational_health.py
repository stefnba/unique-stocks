"""Check production-facing pipeline health from Prefect and lake audit state."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from core.clients.lake import DataLakeClient
from core.utils.redaction import redact_sensitive_query_params
from scripts.health_common import prefect_api_is_healthy


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


def configured_recent_domains(cli_domains: Sequence[str] | None) -> list[str]:
    """Return recent-domain checks from CLI args or OPERATIONAL_HEALTH_RECENT_DOMAINS."""
    if cli_domains is not None:
        return [domain.strip() for domain in cli_domains if domain.strip()]
    return _env_list("OPERATIONAL_HEALTH_RECENT_DOMAINS")


def operational_lake_read_only() -> bool:
    """Return whether operational health should open the lake in read-only mode."""
    value = os.environ.get("OPERATIONAL_HEALTH_LAKE_READ_ONLY", "auto").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    if value not in {"", "auto"}:
        raise ValueError("OPERATIONAL_HEALTH_LAKE_READ_ONLY must be true, false, or auto")
    return not bool(os.environ.get("MOTHERDUCK_TOKEN", "").strip())


def build_parser() -> argparse.ArgumentParser:
    """Build the operational health CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prefect-api-url",
        default=os.environ.get("PREFECT_API_URL", ""),
        help="Prefect API URL. Defaults to PREFECT_API_URL.",
    )
    parser.add_argument(
        "--stale-running-hours",
        type=float,
        default=_env_float("OPERATIONAL_HEALTH_STALE_RUNNING_HOURS", 2.0),
        help="Fail when pipeline.runs has running rows older than this many hours.",
    )
    parser.add_argument(
        "--recent-domain",
        action="append",
        default=None,
        help=(
            "Require at least one recent completed run for this domain. May be passed more than once. "
            "Defaults to OPERATIONAL_HEALTH_RECENT_DOMAINS when omitted."
        ),
    )
    parser.add_argument(
        "--recent-hours",
        type=float,
        default=_env_float("OPERATIONAL_HEALTH_RECENT_HOURS", 36.0),
        help="Freshness window for --recent-domain checks.",
    )
    return parser


def _env_float(name: str, default: float) -> float:
    """Return a float from an environment variable or a default value."""
    value = os.environ.get(name, "").strip()
    if not value:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {value!r}") from exc


def _env_list(name: str) -> list[str]:
    """Return comma- or whitespace-separated environment values."""
    raw = os.environ.get(name, "")
    return [value for item in raw.split(",") for value in item.split() if value]


def main(argv: Sequence[str] | None = None) -> int:
    """Run operational health checks and return a process exit code."""
    try:
        args = build_parser().parse_args(argv)
        lake_read_only = operational_lake_read_only()
    except ValueError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 2
    failures: list[str] = []
    recent_domains = configured_recent_domains(args.recent_domain)

    if not args.prefect_api_url:
        failures.append("PREFECT_API_URL is not set")
    elif not prefect_api_is_healthy(str(args.prefect_api_url)):
        failures.append("Prefect API health endpoint is not reachable")

    now = datetime.now(UTC)
    lake: OperationalHealthLake | None = None
    try:
        lake = DataLakeClient(read_only=lake_read_only, ensure_database=False, schemas=())
        stale_runs = stale_running_runs(lake, older_than=now - timedelta(hours=max(0.0, args.stale_running_hours)))
        if stale_runs:
            failures.append(f"{len(stale_runs)} stale running pipeline run(s)")

        latest_by_domain = recent_domain_runs(
            lake,
            domains=recent_domains,
            since=now - timedelta(hours=max(0.0, args.recent_hours)),
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

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}", file=sys.stderr)
        return 1

    print("OK: Prefect API and lake audit health checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
