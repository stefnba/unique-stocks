"""Parsers for instrument reference Bronze rows."""

from datetime import date

import structlog

from core.ingestion import BronzeParseResult
from domains.instrument.models import InstrumentSnapshot
from providers.eodhd.models import Instrument

log = structlog.get_logger(__name__)


def parse_instrument_snapshot(
    raw: Instrument,
    exchange_code: str,
    snapshot_date: date,
) -> BronzeParseResult[InstrumentSnapshot]:
    """Parse one provider instrument into a Bronze snapshot row."""
    row = InstrumentSnapshot(
        snapshot_date=snapshot_date,
        exchange_code=exchange_code,
        ticker=raw.ticker,
        name=raw.name,
        country=raw.country,
        exchange=raw.exchange,
        currency=raw.currency,
        asset_type=raw.asset_type,
        isin=raw.isin,
    )
    return BronzeParseResult(row=row, raw_fragment=raw)


def parse_instrument_snapshots(
    raws: list[Instrument],
    exchange_code: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[InstrumentSnapshot]], list[Instrument]]:
    """Parse provider instrument with partial success for large payloads."""
    valid: list[BronzeParseResult[InstrumentSnapshot]] = []
    rejected: list[Instrument] = []
    for raw in raws:
        try:
            valid.append(parse_instrument_snapshot(raw, exchange_code, snapshot_date))
        except Exception as exc:
            rejected.append(raw)
            log.warning("instrument.parse_rejected", exchange=exchange_code, ticker=raw.ticker, error=str(exc))
    return valid, rejected
