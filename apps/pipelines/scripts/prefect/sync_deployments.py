"""CLI entrypoint for syncing Prefect deployments."""

from __future__ import annotations

import argparse
import asyncio
import subprocess
from collections.abc import Sequence
from pathlib import Path

import structlog

from core.prefect.deployments import DEFAULT_PREFECT_YAML, sync_deployments

log = structlog.get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the deployment sync CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prefect-yaml",
        type=Path,
        default=DEFAULT_PREFECT_YAML,
        help="Path to the Prefect project manifest.",
    )
    parser.add_argument(
        "--plan",
        action="store_true",
        help="Read Prefect state and print planned deployment changes without writing them.",
    )
    parser.add_argument(
        "--prune-only",
        action="store_true",
        help="Delete orphaned deployments but skip ``prefect deploy --all``.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Prefect deployment sync CLI."""
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        return asyncio.run(
            sync_deployments(
                prefect_yaml=args.prefect_yaml,
                plan=args.plan,
                prune_only=args.prune_only,
            )
        )
    except subprocess.CalledProcessError:
        log.exception("deploy.sync.prefect_deploy_failed")
        return 1
    except Exception:
        log.exception("deploy.sync.failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
