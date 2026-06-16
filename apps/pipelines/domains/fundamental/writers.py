"""Bronze write fan-out for parsed fundamentals slices."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import BronzeParseResult, BronzeWrite
from domains.fundamental.assets import FUNDAMENTAL_NOT_APPLICABLE_REASONS, record_fundamental_bronze_materialization
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
        (
            "fundamental_document",
            write_bronze_fundamental_document(document, source_uri=source_uri),
        ),
        (
            "fundamental_stock_identity",
            write_bronze_fundamental_stock_identity(
                identity,
                source_uri=source_uri,
                provider_instrument_code=provider_instrument_code,
            ),
        ),
        (
            "fundamental_statement_fact",
            write_bronze_fundamental_statement_facts(
                statement_facts,
                provider_instrument_code=provider_instrument_code,
                snapshot_date=snapshot_date,
                source_uri=source_uri,
            ),
        ),
        (
            "fundamental_stock_earnings_fact",
            write_bronze_fundamental_stock_earnings_facts(
                earnings_facts,
                provider_instrument_code=provider_instrument_code,
                snapshot_date=snapshot_date,
                source_uri=source_uri,
            ),
        ),
        (
            "fundamental_stock_shares_stats",
            write_bronze_fundamental_stock_shares_stats(
                shares_stats,
                source_uri=source_uri,
                provider_instrument_code=provider_instrument_code,
            ),
        ),
        (
            "fundamental_stock_outstanding_shares",
            write_bronze_fundamental_stock_outstanding_shares(
                outstanding_shares,
                provider_instrument_code=provider_instrument_code,
                snapshot_date=snapshot_date,
                source_uri=source_uri,
            ),
        ),
        (
            "fundamental_stock_holder",
            write_bronze_fundamental_stock_holders(
                holders,
                provider_instrument_code=provider_instrument_code,
                snapshot_date=snapshot_date,
                source_uri=source_uri,
            ),
        ),
        (
            "fundamental_stock_insider_transaction",
            write_bronze_fundamental_stock_insider_transactions(
                insider_transactions,
                provider_instrument_code=provider_instrument_code,
                snapshot_date=snapshot_date,
                source_uri=source_uri,
            ),
        ),
        (
            "fundamental_stock_metric_fact",
            write_bronze_fundamental_stock_metric_facts(
                metric_facts,
                provider_instrument_code=provider_instrument_code,
                snapshot_date=snapshot_date,
                source_uri=source_uri,
            ),
        ),
        (
            "fundamental_etf_identity",
            write_bronze_fundamental_etf_identity(
                etf_identity,
                source_uri=source_uri,
                provider_instrument_code=provider_instrument_code,
            ),
        ),
        (
            "fundamental_mutual_fund_identity",
            write_bronze_fundamental_mutual_fund_identity(
                mutual_fund_identity,
                source_uri=source_uri,
                provider_instrument_code=provider_instrument_code,
            ),
        ),
        (
            "fundamental_index_identity",
            write_bronze_fundamental_index_identity(
                index_identity,
                source_uri=source_uri,
                provider_instrument_code=provider_instrument_code,
            ),
        ),
        (
            "fundamental_etf_holding",
            write_bronze_fundamental_etf_holdings(
                etf_holdings,
                provider_instrument_code=provider_instrument_code,
                snapshot_date=snapshot_date,
                source_uri=source_uri,
            ),
        ),
        (
            "fundamental_mutual_fund_holding",
            write_bronze_fundamental_mutual_fund_holdings(
                mutual_fund_holdings,
                provider_instrument_code=provider_instrument_code,
                snapshot_date=snapshot_date,
                source_uri=source_uri,
            ),
        ),
        (
            "fundamental_fund_metric_fact",
            write_bronze_fundamental_fund_metric_facts(
                fund_metric_facts,
                provider_instrument_code=provider_instrument_code,
                snapshot_date=snapshot_date,
                source_uri=source_uri,
            ),
        ),
        (
            "fundamental_index_component",
            write_bronze_fundamental_index_components(
                index_components,
                provider_instrument_code=provider_instrument_code,
                snapshot_date=snapshot_date,
                source_uri=source_uri,
            ),
        ),
    ]
    aggregate = FundamentalBronzeSliceWrite(
        rows_written=sum(write.rows_written for _, write in writes),
        reason=_combined_write_reason(*(write.reason for _, write in writes)),
    )
    record_fundamental_bronze_materialization(
        provider="eodhd",
        provider_exchange_code=document.row.provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        snapshot_date=snapshot_date,
        rows_written=aggregate.rows_written,
        reason=aggregate.reason,
        source_uri=source_uri,
        slice_count=len(writes),
        slices_changed=_slice_names_by_kind(writes, changed=True),
        slices_unchanged=_slice_names_by_kind(writes, unchanged=True),
        slices_not_applicable=_slice_names_by_kind(writes, not_applicable=True),
        slice_rows_written={name: write.rows_written for name, write in writes},
    )
    return aggregate


def _combined_write_reason(*reasons: str | None) -> str | None:
    """Combine non-empty Bronze write reasons into a stable unit reason string."""
    reason_set = sorted({reason for reason in reasons if reason})
    return ",".join(reason_set) if reason_set else None


def _slice_names_by_kind(
    writes: list[tuple[str, BronzeWrite]],
    *,
    changed: bool = False,
    unchanged: bool = False,
    not_applicable: bool = False,
) -> list[str]:
    """Return slice names grouped by aggregate materialization semantics."""
    names: list[str] = []
    not_applicable_reasons = set(FUNDAMENTAL_NOT_APPLICABLE_REASONS)
    for name, write in writes:
        reason = write.reason or ""
        is_changed = write.rows_written > 0
        is_not_applicable = write.rows_written == 0 and reason in not_applicable_reasons
        is_unchanged = write.rows_written == 0 and not is_not_applicable
        if (changed and is_changed) or (unchanged and is_unchanged) or (not_applicable and is_not_applicable):
            names.append(name)
    return names


__all__ = ["FundamentalBronzeSliceWrite", "write_fundamental_bronze_slices"]
