"""CLI entrypoint for local smoke presets.

The smoke runner is a local developer convenience layer over the real Prefect
flows. It calls live provider code and the normal landing/Bronze write path,
but passes explicit small parameters so a quick check does not expand to the
full dbt-built provider universe.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
from datetime import date

from orchestration.smoke import SmokePresetRequest, normalize_preset, run_preset

DEFAULT_FUNDAMENTAL_PROVIDER_EXCHANGE_CODE = "US"
DEFAULT_FUNDAMENTAL_PROVIDER_INSTRUMENT_CODE = "AAPL"
DEFAULT_PROVIDER_EXCHANGE_CODE = "XETRA"
DEFAULT_SCHEDULE_EXCHANGE_CODE = "US"

_EXAMPLES = """examples:
  uv run python scripts/smoke/run.py fundamental
  uv run python scripts/smoke/run.py fundamental --exchange US --instrument MSFT
  uv run python scripts/smoke/run.py exchange
  uv run python scripts/smoke/run.py exchange_schedule --exchange XETR
  uv run python scripts/smoke/run.py instrument --exchange US
  uv run python scripts/smoke/run.py eod_price --exchange US

make aliases:
  make smoke FLOW=fundamental
  make smoke FLOW=exchange
  make smoke FLOW=exchange_schedule ARGS="--exchange XETR"
  make smoke FLOW=instrument ARGS="--exchange US"
  make smoke FLOW=eod_price ARGS="--exchange US"
"""


def parse_iso_date(value: str) -> date:
    """Parse a CLI date value in ISO-8601 calendar-date format."""
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected date in YYYY-MM-DD format") from exc


def build_parser() -> argparse.ArgumentParser:
    """Build the smoke preset CLI parser."""
    parser = argparse.ArgumentParser(
        description="Run a narrow local ingestion smoke preset.",
        epilog=_EXAMPLES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="preset", required=True)

    fundamental = subparsers.add_parser("fundamental", help="Run one EODHD fundamentals instrument.")
    fundamental.add_argument(
        "--exchange",
        default=DEFAULT_FUNDAMENTAL_PROVIDER_EXCHANGE_CODE,
        help="Provider exchange code.",
    )
    fundamental.add_argument(
        "--instrument",
        default=DEFAULT_FUNDAMENTAL_PROVIDER_INSTRUMENT_CODE,
        help="Provider instrument code.",
    )
    fundamental.add_argument("--snapshot-date", type=parse_iso_date, default=None, help="Optional YYYY-MM-DD date.")

    subparsers.add_parser("exchange", help="Run exchange catalog and MIC registry refreshes.")

    exchange_schedule = subparsers.add_parser(
        "exchange-schedule",
        aliases=["exchange_schedule"],
        help="Run one provider exchange schedule namespace.",
    )
    exchange_schedule.add_argument(
        "--exchange",
        default=DEFAULT_SCHEDULE_EXCHANGE_CODE,
        help="Provider schedule namespace code.",
    )
    exchange_schedule.add_argument(
        "--snapshot-date",
        type=parse_iso_date,
        default=None,
        help="Optional YYYY-MM-DD date.",
    )

    instrument = subparsers.add_parser("instrument", help="Run one provider instrument namespace.")
    instrument.add_argument("--exchange", default=DEFAULT_PROVIDER_EXCHANGE_CODE, help="Provider namespace code.")
    instrument.add_argument("--snapshot-date", type=parse_iso_date, default=None, help="Optional YYYY-MM-DD date.")

    eod_price = subparsers.add_parser(
        "eod-price",
        aliases=["eod_price"],
        help="Run one provider EOD bulk namespace.",
    )
    eod_price.add_argument("--exchange", default=DEFAULT_PROVIDER_EXCHANGE_CODE, help="Provider namespace code.")
    eod_price.add_argument("--trade-date", type=parse_iso_date, default=None, help="Optional YYYY-MM-DD date.")

    return parser


def request_from_args(args: argparse.Namespace) -> SmokePresetRequest:
    """Build an explicit smoke request from parsed CLI arguments."""
    return SmokePresetRequest(
        preset=normalize_preset(str(args.preset)),
        exchange=getattr(args, "exchange", None),
        instrument=getattr(args, "instrument", None),
        snapshot_date=getattr(args, "snapshot_date", None),
        trade_date=getattr(args, "trade_date", None),
    )


async def main_async(argv: Sequence[str] | None = None) -> int:
    """Parse CLI arguments and run the selected smoke preset."""
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    await run_preset(request_from_args(args))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the smoke preset CLI."""
    return asyncio.run(main_async(argv))


if __name__ == "__main__":
    raise SystemExit(main())
