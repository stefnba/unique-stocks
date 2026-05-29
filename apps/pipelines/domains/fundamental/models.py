"""Bronze models for fundamentals ingestion."""

from datetime import date
from decimal import Decimal

from pydantic import ConfigDict

from core.models import BronzeModel


class FundamentalDocument(BronzeModel):
    """One landed fundamentals document for a ticker snapshot.

    The full provider payload is kept in the standard Bronze ``raw_json``
    envelope. This row stores only stable document-level metadata needed for
    lineage, idempotency, and downstream routing.
    """

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    code: str
    name: str | None = None
    instrument_type: str
    instrument_family: str
    primary_ticker: str | None = None
    provider_listing_exchange_code: str | None = None
    provider_updated_at: date | None = None
    top_level_sections: list[str]
    payload_hash: str


class FundamentalStockIdentitySnapshot(BronzeModel):
    """Stock-specific identity fields from the fundamentals ``General`` section."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    code: str
    name: str | None = None
    primary_ticker: str | None = None
    provider_listing_exchange_code: str | None = None
    currency_code: str | None = None
    currency_name: str | None = None
    country_name: str | None = None
    country_iso: str | None = None
    isin: str | None = None
    cusip: str | None = None
    cik: str | None = None
    lei: str | None = None
    open_figi: str | None = None
    employer_id_number: str | None = None
    fiscal_year_end: str | None = None
    ipo_date: date | None = None
    sector: str | None = None
    industry: str | None = None
    gic_sector: str | None = None
    gic_group: str | None = None
    gic_industry: str | None = None
    gic_sub_industry: str | None = None
    home_category: str | None = None
    is_delisted: bool | None = None
    delisted_date: date | None = None
    full_time_employees: int | None = None
    web_url: str | None = None
    logo_url: str | None = None


class FundamentalStatementFact(BronzeModel):
    """One numeric financial-statement fact from a stock fundamentals document."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    statement_type: str
    period_type: str
    period_end_date: date
    filing_date: date | None = None
    currency_symbol: str | None = None
    metric_name: str
    metric_value: Decimal
