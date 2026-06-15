"""Create Prefect event automations for the pipeline."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence

from control_plane.prefect.automations import PREFECT_AUTOMATIONS


def build_parser() -> argparse.ArgumentParser:
    """Build the Prefect automations setup CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned Prefect automations without changing the server.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint."""
    args = build_parser().parse_args(list(argv) if argv is not None else None)

    try:
        return asyncio.run(PREFECT_AUTOMATIONS.sync(dry_run=args.dry_run))
    except Exception as e:
        print(f"Error syncing automations: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
