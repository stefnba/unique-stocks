"""CLI entrypoint for production-facing operational health checks."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

from core.infrastructure.health.operational import OperationalHealthConfig, run_operational_health


def _env_float(name: str, default: float) -> float:
    """Read a float from the environment with a default."""
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number.") from exc


def _env_list(name: str) -> list[str]:
    """Read a comma/space separated environment variable into a list."""
    raw = os.environ.get(name, "")
    return [part.strip() for chunk in raw.split(",") for part in chunk.split() if part.strip()]


def configured_recent_domains(values: Sequence[str] | None) -> list[str]:
    """Resolve freshness domains from CLI values or environment defaults."""
    if values is not None:
        return [value.strip() for value in values if value.strip()]
    return _env_list("OPERATIONAL_HEALTH_RECENT_DOMAINS")


def operational_lake_read_only(
    value: str | None = None,
    *,
    motherduck_token: str | None = None,
) -> bool:
    """Resolve whether the healthcheck should open the lake in read-only mode."""
    raw = value if value is not None else os.environ.get("OPERATIONAL_HEALTH_LAKE_READ_ONLY", "auto")
    normalized = (raw or "auto").strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    if normalized not in {"", "auto"}:
        raise ValueError("OPERATIONAL_HEALTH_LAKE_READ_ONLY must be true, false, or auto")

    token = motherduck_token if motherduck_token is not None else os.environ.get("MOTHERDUCK_TOKEN", "")
    return not bool(token.strip())


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
        help="Fail when running audit rows are older than this many hours.",
    )
    parser.add_argument(
        "--recent-domain",
        action="append",
        dest="recent_domains",
        help="Require a recent completed run for this domain. Repeatable.",
    )
    parser.add_argument(
        "--recent-hours",
        type=float,
        default=_env_float("OPERATIONAL_HEALTH_RECENT_HOURS", 36.0),
        help="Freshness window for required recent domains.",
    )
    parser.add_argument(
        "--lake-read-only",
        default=os.environ.get("OPERATIONAL_HEALTH_LAKE_READ_ONLY"),
        help="Whether to open the lake in read-only mode: true/false. Defaults depend on MOTHERDUCK_TOKEN.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the operational health CLI."""
    try:
        args = build_parser().parse_args(list(argv) if argv is not None else None)
        lake_read_only = operational_lake_read_only(args.lake_read_only)
    except ValueError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 2

    config = OperationalHealthConfig(
        prefect_api_url=str(args.prefect_api_url),
        stale_running_hours=float(args.stale_running_hours),
        recent_domains=configured_recent_domains(args.recent_domains),
        recent_hours=float(args.recent_hours),
        lake_read_only=lake_read_only,
    )
    result = run_operational_health(config)

    for failure in result.failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    if not result.passed:
        return 1

    print("OK: Prefect API and lake audit health checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
