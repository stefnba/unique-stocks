"""Parsers for instrument reference Bronze rows."""

from datetime import date

import structlog

from core.ingestion import BronzeParseResult
from core.ingestion.parser import parse_best_effort_rows
from domains.instrument.models import InstrumentSnapshot
from providers.eodhd.models import Instrument

log = structlog.get_logger(__name__)


def parse_instrument_snapshots(
    raws: list[Instrument],
    exchange_code: str,
    snapshot_date: date,
) -> tuple[list[BronzeParseResult[InstrumentSnapshot]], list[Instrument]]:
    """Parse provider instrument with partial success for large payloads."""
    return parse_best_effort_rows(
        raws,
        lambda raw: InstrumentSnapshot(
            snapshot_date=snapshot_date,
            exchange_code=exchange_code,
            ticker=raw.ticker,
            name=raw.name,
            country=raw.country,
            exchange=raw.exchange,
            currency=raw.currency,
            asset_type=raw.asset_type,
            isin=raw.isin,
        ),
        on_rejected=lambda raw, exc: log.warning(
            "instrument.parse_rejected",
            exchange=exchange_code,
            ticker=raw.ticker,
            error=str(exc),
        ),
    )
