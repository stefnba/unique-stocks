"""CLI entrypoint for container-local Prefect worker health checks."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from core.infrastructure.health.worker import check_worker_health


def build_parser() -> argparse.ArgumentParser:
    """Build the worker health CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prefect-api-url",
        default=os.environ.get("PREFECT_API_URL", ""),
        help="Prefect API URL. Defaults to PREFECT_API_URL.",
    )
    parser.add_argument(
        "--proc-root",
        type=Path,
        default=Path("/proc"),
        help="Process filesystem root. Default: /proc.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the worker health CLI."""
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    failures = check_worker_health(
        api_url=str(args.prefect_api_url),
        proc_root=args.proc_root,
    )
    for failure in failures:
        print(failure, file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
