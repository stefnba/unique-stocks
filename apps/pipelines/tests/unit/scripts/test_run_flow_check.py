"""Tests for the flow-check runner CLI adapter."""

from datetime import date

from core.orchestration.flow_check import FlowCheckRequest
from scripts.orchestration import run_flow_check as flow_check_cli


def test_flow_check_cli_builds_default_fundamental_request() -> None:
    """Fundamental CLI defaults should stay in the script adapter."""
    args = flow_check_cli.build_parser().parse_args(["fundamental"])

    request = flow_check_cli.request_from_args(args)

    assert request == FlowCheckRequest(
        preset="fundamental",
        exchange="US",
        instrument="AAPL",
    )


def test_flow_check_cli_accepts_domain_alias_and_trade_date() -> None:
    """Domain-style aliases should map to canonical app flow-check presets."""
    args = flow_check_cli.build_parser().parse_args(["eod_price", "--exchange", "US", "--trade-date", "2026-05-31"])

    request = flow_check_cli.request_from_args(args)

    assert request == FlowCheckRequest(
        preset="eod-price",
        exchange="US",
        trade_date=date(2026, 5, 31),
    )
