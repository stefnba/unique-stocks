"""Create all Prefect server controls for the pipeline."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence

from core.prefect.automations import setup_prefect_automations
from core.prefect.limits import setup_prefect_limits
from providers.registry import Provider


def build_parser() -> argparse.ArgumentParser:
    """Build the Prefect controls setup CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned Prefect controls without changing the server.",
    )
    return parser


async def setup_prefect_controls(*, dry_run: bool) -> int:
    """Upsert Prefect limits and event automations."""
    limits_exit = await setup_prefect_limits(providers=Provider, dry_run=dry_run)
    automations_exit = await setup_prefect_automations(dry_run=dry_run)
    return max(limits_exit, automations_exit)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint."""
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    return asyncio.run(setup_prefect_controls(dry_run=args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
