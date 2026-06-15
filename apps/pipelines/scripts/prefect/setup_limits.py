"""Create Prefect global limits and provider rate limits for the pipeline."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence

from control_plane.prefect.limits import PREFECT_LIMITS


def build_parser() -> argparse.ArgumentParser:
    """Build the Prefect limits setup CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned Prefect limits without changing the server.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint."""
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    return asyncio.run(PREFECT_LIMITS.sync(dry_run=args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
