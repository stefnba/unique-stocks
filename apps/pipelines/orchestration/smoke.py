"""Application local smoke presets.

This module intentionally imports concrete orchestration flows and is therefore
app orchestration, not generic operations infrastructure. The presets keep
local developer checks narrow while still exercising the real provider,
landing, and Bronze write paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

import structlog

from config.settings import get_settings
from orchestration.flows.eod_price import eod_price_flow
from orchestration.flows.exchange import exchange_catalog_flow, exchange_mic_registry_flow
from orchestration.flows.exchange_schedule import exchange_schedule_flow
from orchestration.flows.fundamental import fundamental_flow
from orchestration.flows.instrument import instrument_flow

type SmokePreset = Literal["fundamental", "instrument", "eod-price", "exchange", "exchange-schedule"]

_PRESET_ALIASES: dict[str, SmokePreset] = {
    "eod_price": "eod-price",
    "eod-price": "eod-price",
    "exchange": "exchange",
    "exchange_schedule": "exchange-schedule",
    "exchange-schedule": "exchange-schedule",
    "fundamental": "fundamental",
    "instrument": "instrument",
}

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class SmokePresetRequest:
    """Explicit request for one smoke preset run."""

    preset: SmokePreset
    exchange: str | None = None
    instrument: str | None = None
    snapshot_date: date | None = None
    trade_date: date | None = None


def normalize_preset(value: str) -> SmokePreset:
    """Normalize common domain spelling variants to smoke preset names."""
    preset = _PRESET_ALIASES.get(value.strip().lower())
    if preset is not None:
        return preset
    raise ValueError(f"Unsupported smoke preset: {value}")


async def run_preset(request: SmokePresetRequest) -> dict[str, object]:
    """Run a scoped local smoke preset against the real flow implementations."""
    _ensure_not_production()
    preset = normalize_preset(request.preset)
    log.info("smoke.run_start", preset=preset)

    if preset == "fundamental":
        if not request.exchange:
            raise ValueError("exchange is required for fundamental smoke preset")
        if not request.instrument:
            raise ValueError("instrument is required for fundamental smoke preset")
        summary = await fundamental_flow(
            provider_instruments=[
                {
                    "provider_exchange_code": request.exchange,
                    "provider_instrument_code": request.instrument,
                }
            ],
            snapshot_date=request.snapshot_date,
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
        if not request.exchange:
            raise ValueError("exchange is required for exchange-schedule smoke preset")
        summary = await exchange_schedule_flow(
            provider_schedule_exchange_codes=[request.exchange],
            snapshot_date=request.snapshot_date,
        )
    elif preset == "instrument":
        if not request.exchange:
            raise ValueError("exchange is required for instrument smoke preset")
        summary = await instrument_flow(
            provider_exchange_codes=[request.exchange],
            snapshot_date=request.snapshot_date,
        )
    elif preset == "eod-price":
        if not request.exchange:
            raise ValueError("exchange is required for eod-price smoke preset")
        summary = await eod_price_flow(
            provider_exchange_codes=[request.exchange],
            trade_date=request.trade_date,
        )
    else:
        raise ValueError(f"Unsupported smoke preset: {preset}")

    log.info("smoke.run_done", preset=preset, summary=summary)
    return summary


def _ensure_not_production() -> None:
    """Reject smoke presets in production environments."""
    if get_settings().is_production:
        raise RuntimeError("pipelines-smoke is a local/dev tool and cannot run with ENVIRONMENT=prod.")
