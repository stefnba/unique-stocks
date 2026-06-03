"""Check production-facing pipeline health from Prefect and lake audit state."""

from __future__ import annotations

import argparse
import os
import sys
import urllib.request
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.error import URLError

from core.clients.lake import DataLakeClient


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


def prefect_api_is_healthy(api_url: str, *, timeout_seconds: float = 5.0) -> bool:
    """Return whether the configured Prefect API health endpoint responds."""
    health_url = api_url.rstrip("/") + "/health"
    try:
        with urllib.request.urlopen(health_url, timeout=timeout_seconds) as response:
            return 200 <= int(response.status) < 300
    except OSError, URLError:
        return False


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
    """Return the latest terminal run for each required domain since the threshold."""
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
              AND status IN ('completed', 'partial', 'skipped')
              AND completed_at >= ?
            ORDER BY completed_at DESC
            LIMIT 1
            """,
            [domain, since],
        )
    return latest


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
        default=2.0,
        help="Fail when pipeline.runs has running rows older than this many hours.",
    )
    parser.add_argument(
        "--recent-domain",
        action="append",
        default=[],
        help="Require at least one recent terminal run for this domain. May be passed more than once.",
    )
    parser.add_argument(
        "--recent-hours",
        type=float,
        default=36.0,
        help="Freshness window for --recent-domain checks.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run operational health checks and return a process exit code."""
    args = build_parser().parse_args(argv)
    failures: list[str] = []

    if not args.prefect_api_url:
        failures.append("PREFECT_API_URL is not set")
    elif not prefect_api_is_healthy(str(args.prefect_api_url)):
        failures.append("Prefect API health endpoint is not reachable")

    now = datetime.now(UTC)
    lake: OperationalHealthLake | None = None
    try:
        lake = DataLakeClient(read_only=True)
        stale_runs = stale_running_runs(lake, older_than=now - timedelta(hours=max(0.0, args.stale_running_hours)))
        if stale_runs:
            failures.append(f"{len(stale_runs)} stale running pipeline run(s)")

        latest_by_domain = recent_domain_runs(
            lake,
            domains=args.recent_domain,
            since=now - timedelta(hours=max(0.0, args.recent_hours)),
        )
        missing_domains = [domain for domain, row in latest_by_domain.items() if row is None]
        if missing_domains:
            failures.append(f"missing recent terminal run for domain(s): {', '.join(missing_domains)}")
    except Exception as exc:
        failures.append(f"lake audit health check failed ({type(exc).__name__})")
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
