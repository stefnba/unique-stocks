"""Bronze write fan-out for parsed fundamentals slices."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import BronzeParseResult
from domains.fundamental.models import (
    FundamentalDocument,
    FundamentalEtfHolding,
    FundamentalEtfIdentitySnapshot,
    FundamentalFundMetricFact,
    FundamentalIndexComponent,
    FundamentalIndexIdentitySnapshot,
    FundamentalMutualFundHolding,
    FundamentalMutualFundIdentitySnapshot,
    FundamentalStatementFact,
    FundamentalStockEarningsFact,
    FundamentalStockHolder,
    FundamentalStockIdentitySnapshot,
    FundamentalStockInsiderTransaction,
    FundamentalStockMetricFact,
    FundamentalStockOutstandingShares,
    FundamentalStockSharesStatsSnapshot,
)
from domains.fundamental.tasks import (
    write_bronze_fundamental_document,
    write_bronze_fundamental_etf_holdings,
    write_bronze_fundamental_etf_identity,
    write_bronze_fundamental_fund_metric_facts,
    write_bronze_fundamental_index_components,
    write_bronze_fundamental_index_identity,
    write_bronze_fundamental_mutual_fund_holdings,
    write_bronze_fundamental_mutual_fund_identity,
    write_bronze_fundamental_statement_facts,
    write_bronze_fundamental_stock_earnings_facts,
    write_bronze_fundamental_stock_holders,
    write_bronze_fundamental_stock_identity,
    write_bronze_fundamental_stock_insider_transactions,
    write_bronze_fundamental_stock_metric_facts,
    write_bronze_fundamental_stock_outstanding_shares,
    write_bronze_fundamental_stock_shares_stats,
)


@dataclass(frozen=True, slots=True)
class FundamentalBronzeSliceWrite:
    """Aggregate result for all Bronze slice writes from one fundamentals document."""

    rows_written: int
    reason: str | None


def write_fundamental_bronze_slices(
    *,
    document: BronzeParseResult[FundamentalDocument],
    identity: BronzeParseResult[FundamentalStockIdentitySnapshot] | None,
    statement_facts: list[BronzeParseResult[FundamentalStatementFact]],
    earnings_facts: list[BronzeParseResult[FundamentalStockEarningsFact]],
    shares_stats: BronzeParseResult[FundamentalStockSharesStatsSnapshot] | None,
    outstanding_shares: list[BronzeParseResult[FundamentalStockOutstandingShares]],
    holders: list[BronzeParseResult[FundamentalStockHolder]],
    insider_transactions: list[BronzeParseResult[FundamentalStockInsiderTransaction]],
    metric_facts: list[BronzeParseResult[FundamentalStockMetricFact]],
    etf_identity: BronzeParseResult[FundamentalEtfIdentitySnapshot] | None,
    mutual_fund_identity: BronzeParseResult[FundamentalMutualFundIdentitySnapshot] | None,
    index_identity: BronzeParseResult[FundamentalIndexIdentitySnapshot] | None,
    etf_holdings: list[BronzeParseResult[FundamentalEtfHolding]],
    mutual_fund_holdings: list[BronzeParseResult[FundamentalMutualFundHolding]],
    fund_metric_facts: list[BronzeParseResult[FundamentalFundMetricFact]],
    index_components: list[BronzeParseResult[FundamentalIndexComponent]],
    provider_instrument_code: str,
    snapshot_date: date,
    source_uri: str,
) -> FundamentalBronzeSliceWrite:
    """Write all modeled fundamentals Bronze slices for one parsed document."""
    writes = [
        write_bronze_fundamental_document(document, source_uri=source_uri),
        write_bronze_fundamental_stock_identity(
            identity,
            source_uri=source_uri,
            provider_instrument_code=provider_instrument_code,
        ),
        write_bronze_fundamental_statement_facts(
            statement_facts,
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        ),
        write_bronze_fundamental_stock_earnings_facts(
            earnings_facts,
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        ),
        write_bronze_fundamental_stock_shares_stats(
            shares_stats,
            source_uri=source_uri,
            provider_instrument_code=provider_instrument_code,
        ),
        write_bronze_fundamental_stock_outstanding_shares(
            outstanding_shares,
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        ),
        write_bronze_fundamental_stock_holders(
            holders,
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        ),
        write_bronze_fundamental_stock_insider_transactions(
            insider_transactions,
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        ),
        write_bronze_fundamental_stock_metric_facts(
            metric_facts,
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        ),
        write_bronze_fundamental_etf_identity(
            etf_identity,
            source_uri=source_uri,
            provider_instrument_code=provider_instrument_code,
        ),
        write_bronze_fundamental_mutual_fund_identity(
            mutual_fund_identity,
            source_uri=source_uri,
            provider_instrument_code=provider_instrument_code,
        ),
        write_bronze_fundamental_index_identity(
            index_identity,
            source_uri=source_uri,
            provider_instrument_code=provider_instrument_code,
        ),
        write_bronze_fundamental_etf_holdings(
            etf_holdings,
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        ),
        write_bronze_fundamental_mutual_fund_holdings(
            mutual_fund_holdings,
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        ),
        write_bronze_fundamental_fund_metric_facts(
            fund_metric_facts,
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        ),
        write_bronze_fundamental_index_components(
            index_components,
            provider_instrument_code=provider_instrument_code,
            snapshot_date=snapshot_date,
            source_uri=source_uri,
        ),
    ]
    return FundamentalBronzeSliceWrite(
        rows_written=sum(write.rows_written for write in writes),
        reason=_combined_write_reason(*(write.reason for write in writes)),
    )


def _combined_write_reason(*reasons: str | None) -> str | None:
    """Combine non-empty Bronze write reasons into a stable unit reason string."""
    reason_set = sorted({reason for reason in reasons if reason})
    return ",".join(reason_set) if reason_set else None


__all__ = ["FundamentalBronzeSliceWrite", "write_fundamental_bronze_slices"]
