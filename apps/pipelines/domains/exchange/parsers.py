"""Parsers for exchange reference Bronze rows."""

import csv
import io
from datetime import date

from core.ingestion import BronzeParseResult
from core.ingestion.parser import parse_strict_rows
from domains.exchange.models import ExchangeCatalogSnapshot, ExchangeMicRegistrySnapshot
from providers.eodhd.models import SupportedExchange
from providers.iso10383.models import ISO10383MICRaw


def parse_exchange_catalog_snapshots(
    raws: list[SupportedExchange],
    snapshot_date: date,
) -> list[BronzeParseResult[ExchangeCatalogSnapshot]]:
    """Parse provider exchange catalog rows into Bronze snapshot results."""
    return parse_strict_rows(
        raws,
        lambda raw: ExchangeCatalogSnapshot(
            snapshot_date=snapshot_date,
            provider_exchange_code=raw.provider_exchange_code,
            name=raw.name,
            operating_mic_codes=raw.operating_mic_codes,
            country=raw.country,
            currency=raw.currency,
            country_iso2=raw.country_iso2,
            country_iso3=raw.country_iso3,
        ),
    )


def load_iso10383_mic_csv(csv_text: str) -> list[dict[str, str]]:
    """Load ISO 10383 CSV text into raw row dictionaries."""
    return list(csv.DictReader(io.StringIO(csv_text)))


def parse_iso10383_mic_raw_rows(
    raw_rows: list[dict[str, str]],
) -> tuple[list[ISO10383MICRaw], list[dict[str, str]]]:
    """Validate raw ISO 10383 CSV rows while preserving rejected raw rows."""
    valid: list[ISO10383MICRaw] = []
    rejected: list[dict[str, str]] = []
    for raw in raw_rows:
        try:
            valid.append(ISO10383MICRaw.model_validate(raw))
        except Exception:
            rejected.append(raw)
    return valid, rejected


def parse_exchange_mic_registry_snapshots(
    raws: list[ISO10383MICRaw],
    snapshot_date: date,
) -> list[BronzeParseResult[ExchangeMicRegistrySnapshot]]:
    """Parse ISO MIC registry rows into Bronze snapshot results."""
    return parse_strict_rows(
        raws,
        lambda raw: ExchangeMicRegistrySnapshot(
            snapshot_date=snapshot_date,
            mic=raw.mic,
            operating_mic=raw.operating_mic,
            mic_type=raw.mic_type,
            name=raw.name,
            legal_entity_name=raw.legal_entity_name,
            lei=raw.lei,
            market_category_code=raw.market_category_code,
            acronym=raw.acronym,
            country_iso2=raw.country_iso2,
            city=raw.city,
            website=raw.website,
            status=raw.status,
            creation_date=raw.creation_date,
            last_update_date=raw.last_update_date,
            last_validation_date=raw.last_validation_date,
            expiry_date=raw.expiry_date,
            comments=raw.comments,
            provider_supported=True,
        ),
    )
