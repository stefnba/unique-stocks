"""CLI entrypoint for saving app Prefect blocks."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import cast

from control_plane.prefect import BlockRegistry
from core.infrastructure.blocks import ExistsMode


def build_parser() -> argparse.ArgumentParser:
    """Build the Prefect block save CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--if-exists",
        choices=("skip", "throw", "overwrite"),
        default="overwrite",
        help="How to handle blocks that already exist. Default: overwrite.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Prefect block save CLI."""
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    BlockRegistry.save_all(if_exists=cast(ExistsMode, args.if_exists))
    return 0
