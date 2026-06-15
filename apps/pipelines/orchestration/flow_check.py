"""Application local flow-check presets.

This module intentionally imports concrete orchestration flows and is therefore
app orchestration, not generic operations infrastructure. The presets keep
local developer checks narrow while still exercising the real provider,
landing, and Bronze write paths.
"""

from __future__ import annotations

from config.domains import Domain
from config.settings import get_settings
from core.orchestration.flow_check import FlowCheckRegistry, FlowCheckRequest, require_request_value
from orchestration.flows.eod_price import eod_price_flow
from orchestration.flows.exchange import exchange_catalog_flow, exchange_mic_registry_flow
from orchestration.flows.exchange_schedule import exchange_schedule_flow
from orchestration.flows.fundamental import fundamental_flow
from orchestration.flows.instrument import instrument_flow

FLOW_CHECK_DOMAINS = (
    Domain.FUNDAMENTAL,
    Domain.EXCHANGE,
    Domain.EXCHANGE_SCHEDULE,
    Domain.INSTRUMENT,
    Domain.EOD_PRICE,
)


def _preset_name(domain: Domain) -> str:
    """Return the command preset name for a domain."""
    return domain.value.replace("_", "-")


PRESET_ALIASES = {domain.value: _preset_name(domain) for domain in FLOW_CHECK_DOMAINS}


async def _run_fundamental_check(request: FlowCheckRequest) -> dict[str, object]:
    """Run one fundamentals instrument through the real flow path."""
    exchange = require_request_value(request.exchange, field="exchange", preset=request.preset)
    instrument = require_request_value(request.instrument, field="instrument", preset=request.preset)
    return await fundamental_flow(
        provider_instruments=[
            {
                "provider_exchange_code": exchange,
                "provider_instrument_code": instrument,
            }
        ],
        snapshot_date=request.snapshot_date,
        batch_size=1,
        max_provider_credits=10,
    )


async def _run_exchange_check(_request: FlowCheckRequest) -> dict[str, object]:
    """Run the exchange catalog and MIC registry reference refreshes."""
    catalog_rows_written = await exchange_catalog_flow()
    mic_summary = await exchange_mic_registry_flow()
    return {
        "exchange_catalog_rows_written": catalog_rows_written,
        "exchange_mic_registry": mic_summary,
    }


async def _run_exchange_schedule_check(request: FlowCheckRequest) -> dict[str, object]:
    """Run one provider exchange schedule namespace through the real flow path."""
    exchange = require_request_value(request.exchange, field="exchange", preset=request.preset)
    return await exchange_schedule_flow(
        provider_schedule_exchange_codes=[exchange],
        snapshot_date=request.snapshot_date,
    )


async def _run_instrument_check(request: FlowCheckRequest) -> dict[str, object]:
    """Run one provider instrument namespace through the real flow path."""
    exchange = require_request_value(request.exchange, field="exchange", preset=request.preset)
    return await instrument_flow(
        provider_exchange_codes=[exchange],
        snapshot_date=request.snapshot_date,
    )


async def _run_eod_price_check(request: FlowCheckRequest) -> dict[str, object]:
    """Run one provider EOD bulk namespace through the real flow path."""
    exchange = require_request_value(request.exchange, field="exchange", preset=request.preset)
    return await eod_price_flow(
        provider_exchange_codes=[exchange],
        trade_date=request.trade_date,
    )


FLOW_CHECK_REGISTRY = FlowCheckRegistry(
    aliases=PRESET_ALIASES,
    handlers={
        _preset_name(Domain.FUNDAMENTAL): _run_fundamental_check,
        _preset_name(Domain.EXCHANGE): _run_exchange_check,
        _preset_name(Domain.EXCHANGE_SCHEDULE): _run_exchange_schedule_check,
        _preset_name(Domain.INSTRUMENT): _run_instrument_check,
        _preset_name(Domain.EOD_PRICE): _run_eod_price_check,
    },
    guard=lambda: _ensure_not_production(),
)


def normalize_preset(value: str) -> str:
    """Normalize common domain spelling variants to flow-check preset names."""
    return FLOW_CHECK_REGISTRY.normalize_preset(value)


async def run_flow_check(request: FlowCheckRequest) -> dict[str, object]:
    """Run a scoped local flow check against the real flow implementations."""
    return await FLOW_CHECK_REGISTRY.run(request)


def _ensure_not_production() -> None:
    """Reject flow checks in production environments."""
    if get_settings().is_production:
        raise RuntimeError("pipelines-flow-check is a local/dev tool and cannot run with ENVIRONMENT=prod.")
