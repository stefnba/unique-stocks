"""Run narrow local ingestion smoke presets.

This script is a local developer convenience layer over the real Prefect flows.
It calls live provider code and the normal landing/Bronze write path, but passes
explicit small parameters so a quick check does not expand to the full dbt-built
provider universe.

Use it for questions like "does this flow still run end-to-end for one ticker or
one provider namespace from my current checkout?" Scoped presets pass explicit
flow parameters, so they do not require the dbt provider-universe view. Do not
use this script as a production scheduler or as a replacement for Prefect
deployment parameters.

Default presets:

- ``fundamental`` fetches one explicit fundamentals ticker, ``AAPL.US``, with a
  one-call credit cap.
- ``instrument`` fetches one provider namespace, ``XETRA``.
- ``eod-price`` / ``eod_price`` fetches one provider namespace, ``XETRA``.
- ``exchange`` refreshes the full provider exchange catalog and ISO MIC
  registry. It is intentionally broader than the one-namespace presets because
  these reference snapshots are the bootstrap inputs for the exchange Silver
  universe.
- ``exchange-schedule`` / ``exchange_schedule`` fetches one provider schedule
  namespace, ``US``.

Examples:
    uv run python scripts/run_smoke.py fundamental
    uv run python scripts/run_smoke.py fundamental --ticker MSFT.US
    uv run python scripts/run_smoke.py exchange
    uv run python scripts/run_smoke.py exchange_schedule --exchange XETR
    uv run python scripts/run_smoke.py instrument --exchange US
    uv run python scripts/run_smoke.py eod_price --exchange US
"""

import argparse
import asyncio
from collections.abc import Sequence
from datetime import date
from typing import Literal

import structlog

from config.settings import get_settings
from domains.eod_price.flows import eod_price_flow
from domains.exchange.flows import exchange_catalog_flow, exchange_mic_registry_flow
from domains.exchange_schedule.flows import exchange_schedule_flow
from domains.fundamental.flows import fundamental_flow
from domains.instrument.flows import instrument_flow

type SmokePreset = Literal["fundamental", "instrument", "eod-price", "exchange", "exchange-schedule"]

DEFAULT_FUNDAMENTAL_TICKER = "AAPL.US"
DEFAULT_PROVIDER_EXCHANGE_CODE = "XETRA"
DEFAULT_SCHEDULE_EXCHANGE_CODE = "US"

log = structlog.get_logger(__name__)

_EXAMPLES = """examples:
  uv run python scripts/run_smoke.py fundamental
  uv run python scripts/run_smoke.py fundamental --ticker MSFT.US
  uv run python scripts/run_smoke.py exchange
  uv run python scripts/run_smoke.py exchange_schedule --exchange XETR
  uv run python scripts/run_smoke.py instrument --exchange US
  uv run python scripts/run_smoke.py eod_price --exchange US

make aliases:
  make smoke FLOW=fundamental
  make smoke FLOW=exchange
  make smoke FLOW=exchange_schedule ARGS="--exchange XETR"
  make smoke FLOW=instrument ARGS="--exchange US"
  make smoke FLOW=eod_price ARGS="--exchange US"
"""


def parse_iso_date(value: str) -> date:
    """Parse a CLI date value in ISO-8601 calendar-date format.

    The smoke presets intentionally accept absolute dates only. That keeps
    repeated local checks reproducible and avoids hiding date arithmetic inside
    a helper script.
    """
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected date in YYYY-MM-DD format") from exc


def build_parser() -> argparse.ArgumentParser:
    """Build the smoke preset CLI parser.

    The parser owns only local developer ergonomics. The production flow
    contracts remain in ``domains/*/flows.py`` and Prefect deployment
    configuration.
    """
    parser = argparse.ArgumentParser(
        description="Run a narrow local ingestion smoke preset.",
        epilog=_EXAMPLES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="preset", required=True)

    fundamental = subparsers.add_parser("fundamental", help="Run one EODHD fundamentals ticker.")
    fundamental.add_argument("--ticker", default=DEFAULT_FUNDAMENTAL_TICKER, help="Exchange-qualified ticker.")
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


def normalize_preset(value: str) -> SmokePreset:
    """Normalize common domain spelling variants to smoke preset names.

    Canonical CLI subcommands use hyphens, while Python domain packages use
    underscores. Accepting both keeps the Makefile and direct CLI calls natural
    for different contexts.
    """
    if value == "eod_price":
        return "eod-price"
    if value == "exchange_schedule":
        return "exchange-schedule"
    if value == "fundamental":
        return "fundamental"
    if value == "exchange":
        return "exchange"
    if value == "exchange-schedule":
        return "exchange-schedule"
    if value == "instrument":
        return "instrument"
    if value == "eod-price":
        return "eod-price"
    raise ValueError(f"Unsupported smoke preset: {value}")


async def run_preset(args: argparse.Namespace) -> dict[str, object]:
    """Run the smoke preset represented by parsed CLI arguments.

    Each branch passes explicit scoping parameters to the real flow:

    - fundamentals uses ``tickers`` rather than provider-universe discovery;
    - exchange runs the two reference refreshes directly;
    - exchange schedule uses ``provider_schedule_exchange_codes`` with one code;
    - instrument uses ``provider_exchange_codes`` with one namespace;
    - EOD price uses ``provider_exchange_codes`` with one namespace.

    That separation is intentional: the flow code stays production-oriented,
    while this script carries local smoke defaults.
    """
    _ensure_not_production()
    preset = normalize_preset(str(args.preset))
    log.info("smoke.run_start", preset=preset)

    if preset == "fundamental":
        summary = await fundamental_flow(
            tickers=[str(args.ticker)],
            snapshot_date=args.snapshot_date,
            batch_size=1,
            max_provider_credits=10,
        )
    elif preset == "exchange":
        catalog_rows_written = await exchange_catalog_flow()
        mic_summary = await exchange_mic_registry_flow()
        summary = {
            "exchange_catalog_rows_written": catalog_rows_written,
            "exchange_mic_registry": mic_summary,
        }
    elif preset == "exchange-schedule":
        summary = await exchange_schedule_flow(
            provider_schedule_exchange_codes=[str(args.exchange)],
            snapshot_date=args.snapshot_date,
        )
    elif preset == "instrument":
        summary = await instrument_flow(
            provider_exchange_codes=[str(args.exchange)],
            snapshot_date=args.snapshot_date,
        )
    elif preset == "eod-price":
        summary = await eod_price_flow(
            provider_exchange_codes=[str(args.exchange)],
            trade_date=args.trade_date,
        )
    else:
        raise ValueError(f"Unsupported smoke preset: {preset}")

    log.info("smoke.run_done", preset=preset, summary=summary)
    return summary


def _ensure_not_production() -> None:
    """Reject smoke presets in production environments."""
    if get_settings().is_production:
        raise RuntimeError("run_smoke.py is a local/dev tool and cannot run with ENVIRONMENT=prod.")


async def main_async(argv: Sequence[str] | None = None) -> int:
    """Parse CLI arguments and run the selected smoke preset."""
    args = build_parser().parse_args(argv)
    await run_preset(args)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for ``uv run python scripts/run_smoke.py ...``."""
    return asyncio.run(main_async(argv))


if __name__ == "__main__":
    raise SystemExit(main())
