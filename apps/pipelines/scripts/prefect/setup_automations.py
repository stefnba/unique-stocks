"""Create Prefect event automations for the pipeline."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence

from core.prefect.automations import setup_prefect_automations


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
    return asyncio.run(setup_prefect_automations(dry_run=args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
