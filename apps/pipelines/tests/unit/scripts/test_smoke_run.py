"""Tests for the smoke runner CLI adapter."""

from datetime import date

from orchestration.smoke import SmokePresetRequest
from scripts.smoke import run as smoke_cli


def test_smoke_cli_builds_default_fundamental_request() -> None:
    """Fundamental CLI defaults should stay in the script adapter."""
    args = smoke_cli.build_parser().parse_args(["fundamental"])

    request = smoke_cli.request_from_args(args)

    assert request == SmokePresetRequest(
        preset="fundamental",
        exchange="US",
        instrument="AAPL",
    )


def test_smoke_cli_accepts_domain_alias_and_trade_date() -> None:
    """Domain-style aliases should map to canonical app smoke presets."""
    args = smoke_cli.build_parser().parse_args(["eod_price", "--exchange", "US", "--trade-date", "2026-05-31"])

    request = smoke_cli.request_from_args(args)

    assert request == SmokePresetRequest(
        preset="eod-price",
        exchange="US",
        trade_date=date(2026, 5, 31),
    )
