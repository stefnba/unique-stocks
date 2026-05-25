"""Parsers for exchange reference Bronze rows."""

from datetime import date

from core.ingestion import BronzeParseResult
from core.ingestion.parser import parse_strict_rows
from domains.exchange.models import ExchangeSnapshot
from providers.eodhd.models import SupportedExchange


def parse_exchange_snapshots(
    raws: list[SupportedExchange],
    snapshot_date: date,
) -> list[BronzeParseResult[ExchangeSnapshot]]:
    """Parse provider exchange rows into Bronze snapshot results."""
    return parse_strict_rows(
        raws,
        lambda raw: ExchangeSnapshot(
            snapshot_date=snapshot_date,
            exchange_code=raw.exchange_code,
            name=raw.name,
            operating_mic=raw.operating_mic,
            country=raw.country,
            currency=raw.currency,
            country_iso2=raw.country_iso2,
            country_iso3=raw.country_iso3,
        ),
    )
