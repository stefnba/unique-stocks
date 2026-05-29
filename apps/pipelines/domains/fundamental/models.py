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


class FundamentalStockEarningsFact(BronzeModel):
    """One numeric earnings fact from a stock fundamentals document."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    earnings_section: str
    period_type: str | None = None
    fiscal_period_end: date
    report_date: date | None = None
    before_after_market: str | None = None
    currency_code: str | None = None
    fiscal_quarter: str | None = None
    period_offset: str | None = None
    metric_name: str
    metric_value: Decimal


class FundamentalStockSharesStatsSnapshot(BronzeModel):
    """One stock share-statistics snapshot from the fundamentals document."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    shares_outstanding: Decimal | None = None
    shares_float: Decimal | None = None
    percent_insiders: Decimal | None = None
    percent_institutions: Decimal | None = None
    shares_short: Decimal | None = None
    shares_short_prior_month: Decimal | None = None
    short_ratio: Decimal | None = None
    short_percent_outstanding: Decimal | None = None
    short_percent_float: Decimal | None = None


class FundamentalStockOutstandingShares(BronzeModel):
    """One historical outstanding-shares observation for a stock."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    period_type: str
    provider_period_label: str
    period_end_date: date
    shares_mln: Decimal | None = None
    shares: Decimal | None = None


class FundamentalStockHolder(BronzeModel):
    """One stock holder row from the fundamentals Holders section."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    holder_type: str
    provider_position: int | None = None
    holder_name: str
    report_date: date
    total_shares_percent: Decimal | None = None
    total_assets_percent: Decimal | None = None
    current_shares: Decimal | None = None
    shares_change: Decimal | None = None
    shares_change_percent: Decimal | None = None


class FundamentalStockSplitsDividendsSnapshot(BronzeModel):
    """One stock splits/dividends snapshot from fundamentals."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    forward_annual_dividend_rate: Decimal | None = None
    forward_annual_dividend_yield: Decimal | None = None
    payout_ratio: Decimal | None = None
    dividend_date: date | None = None
    ex_dividend_date: date | None = None
    last_split_factor: str | None = None
    last_split_date: date | None = None


class FundamentalStockDividendCount(BronzeModel):
    """One yearly dividend-count observation from fundamentals."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    year: int
    dividend_count: int


class FundamentalStockMetricFact(BronzeModel):
    """One numeric stock metric from compact fundamentals sections."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    metric_group: str
    metric_name: str
    metric_value: Decimal
    metric_date: date | None = None


class FundamentalStockEsgActivity(BronzeModel):
    """One ESG activity-involvement row from fundamentals."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    rating_date: date | None = None
    activity: str
    involvement: str | None = None


class FundamentalEtfIdentitySnapshot(BronzeModel):
    """ETF-specific identity fields from ETF fundamentals."""

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
    open_figi: str | None = None
    company_name: str | None = None
    company_url: str | None = None
    etf_url: str | None = None
    domicile: str | None = None
    index_name: str | None = None
    inception_date: date | None = None
    dividend_paying_frequency: str | None = None
    holdings_count: int | None = None


class FundamentalMutualFundIdentitySnapshot(BronzeModel):
    """Mutual-fund-specific identity fields from fund fundamentals."""

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
    open_figi: str | None = None
    fund_category: str | None = None
    fund_family: str | None = None
    fund_style: str | None = None
    fiscal_year_end: str | None = None
    domicile: str | None = None
    inception_date: date | None = None
    update_date: date | None = None


class FundamentalIndexIdentitySnapshot(BronzeModel):
    """Index-specific identity fields from index fundamentals."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    code: str
    name: str | None = None
    provider_listing_exchange_code: str | None = None
    currency_code: str | None = None
    currency_name: str | None = None
    country_name: str | None = None
    country_iso: str | None = None
    open_figi: str | None = None
    market_cap: Decimal | None = None


class FundamentalEtfHolding(BronzeModel):
    """One ETF holding edge from ETF fundamentals."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    holding_symbol: str
    holding_code: str | None = None
    holding_exchange: str | None = None
    holding_name: str | None = None
    sector: str | None = None
    industry: str | None = None
    country: str | None = None
    region: str | None = None
    assets_percent: Decimal | None = None
    is_top_10: bool


class FundamentalMutualFundHolding(BronzeModel):
    """One mutual fund top holding from fund fundamentals."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    provider_position: int | None = None
    holding_name: str
    weight_percent: Decimal | None = None


class FundamentalFundMetricFact(BronzeModel):
    """One numeric ETF/fund metric fact from nested provider fund sections."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    instrument_family: str
    metric_group: str
    metric_category: str | None = None
    metric_name: str
    metric_value: Decimal
    metric_date: date | None = None


class FundamentalIndexComponent(BronzeModel):
    """One current index constituent edge."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    provider_position: int | None = None
    component_code: str
    component_exchange: str | None = None
    component_ticker: str | None = None
    component_name: str | None = None
    sector: str | None = None
    industry: str | None = None
    weight: Decimal | None = None


class FundamentalIndexHistoricalComponent(BronzeModel):
    """One historical index constituent membership edge."""

    model_config = ConfigDict(strict=True)

    snapshot_date: date
    provider_exchange_code: str
    ticker: str
    provider_position: int | None = None
    component_code: str
    component_name: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    is_active_now: bool | None = None
    is_delisted: bool | None = None
