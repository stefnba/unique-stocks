"""Fundamentals ingestion flow."""

import asyncio
from datetime import date

import structlog
from prefect import flow
from pydantic import ValidationError

from core.ingestion import PipelineRunScope, PipelineRunTracker, RejectionRecord, terminal_status
from domains.fundamental.tasks import (
    delete_fundamental_snapshot_rows,
    fetch_fundamental_ticker,
    fundamental_document_already_ingested,
    load_fundamental_document_payload_hash,
    load_fundamental_stock_tickers,
    parse_fundamental_stock,
    write_bronze_fundamental_document,
    write_bronze_fundamental_etf_holdings,
    write_bronze_fundamental_etf_identity,
    write_bronze_fundamental_fund_metric_facts,
    write_bronze_fundamental_index_components,
    write_bronze_fundamental_index_historical_components,
    write_bronze_fundamental_index_identity,
    write_bronze_fundamental_mutual_fund_holdings,
    write_bronze_fundamental_mutual_fund_identity,
    write_bronze_fundamental_statement_facts,
    write_bronze_fundamental_stock_dividend_counts,
    write_bronze_fundamental_stock_earnings_facts,
    write_bronze_fundamental_stock_esg_activities,
    write_bronze_fundamental_stock_holders,
    write_bronze_fundamental_stock_identity,
    write_bronze_fundamental_stock_metric_facts,
    write_bronze_fundamental_stock_outstanding_shares,
    write_bronze_fundamental_stock_shares_stats,
    write_bronze_fundamental_stock_splits_dividends,
    write_fundamental_to_landing,
)

log = structlog.get_logger(__name__)
_REJECTION_SAMPLE_LIMIT_PER_TICKER = 100


@flow(
    name="fundamental-quarterly",
    description="Ingest stock fundamentals documents and normalized statement facts from EODHD.",
)
async def fundamental_flow(
    tickers: list[str] | None = None,
    snapshot_date: date | None = None,
    provider_exchange_codes: list[str] | None = None,
    limit: int | None = None,
    skip_existing: bool = True,
    refresh_existing: bool = False,
) -> dict[str, object]:
    """Ingest fundamentals for explicit tickers or latest stock instruments.

    This first implementation supports stock fundamentals. Non-stock documents
    still produce a document metadata row, but family-specific ETF/fund/index
    extraction is intentionally left for separate parser slices with fixtures.
    ``snapshot_date`` is an ingestion batch date. By default, already-ingested
    ticker snapshots are skipped before fetching to avoid spending provider
    credits. Set ``refresh_existing=True`` to fetch existing ticker snapshots,
    compare payload hashes, and replace same-day Bronze rows only when the
    provider document changed. Passing ``skip_existing=False`` uses the same
    changed-payload refresh behavior.
    """
    snapshot_date = snapshot_date or date.today()
    requested_tickers = tickers or load_fundamental_stock_tickers(provider_exchange_codes, limit)
    refresh_changed_existing = refresh_existing or not skip_existing

    summary: dict = {
        "snapshot_date": snapshot_date.isoformat(),
        "tickers": {},
        "skipped": [],
        "failed": [],
    }
    total_raw = 0
    total_valid = 0
    total_rejected = 0
    total_written = 0

    tracker = PipelineRunTracker()
    with tracker.track_run(
        flow_name="fundamental-quarterly",
        domain="fundamental",
        run_kind="snapshot",
        provider="eodhd",
        parameters={
            "tickers": tickers,
            "snapshot_date": snapshot_date.isoformat(),
            "provider_exchange_codes": provider_exchange_codes,
            "limit": limit,
            "skip_existing": skip_existing,
            "refresh_existing": refresh_existing,
            "refresh_changed_existing": refresh_changed_existing,
        },
        target_window_start=snapshot_date,
        target_window_end=snapshot_date,
    ) as run:
        try:
            for ticker in requested_tickers:
                unit_key: dict[str, object] = {"ticker": ticker, "snapshot_date": snapshot_date.isoformat()}
                if skip_existing and not refresh_changed_existing and fundamental_document_already_ingested(
                    ticker, snapshot_date
                ):
                    summary["skipped"].append(ticker)
                    run.record_unit(
                        unit_type="ticker_snapshot",
                        unit_key=unit_key,
                        status="skipped",
                        reason="already_ingested",
                        rows_written=0,
                    )
                    continue

                try:
                    raw = await fetch_fundamental_ticker(ticker)
                    (
                        document,
                        identity,
                        statement_facts,
                        earnings_facts,
                        shares_stats,
                        outstanding_shares,
                        holders,
                        splits_dividends,
                        dividend_counts,
                        metric_facts,
                        esg_activities,
                        etf_identity,
                        mutual_fund_identity,
                        index_identity,
                        etf_holdings,
                        mutual_fund_holdings,
                        fund_metric_facts,
                        index_components,
                        index_historical_components,
                        rejected_rows,
                    ) = parse_fundamental_stock(raw, ticker, snapshot_date)
                    rows_valid = (
                        1
                        + (1 if identity is not None else 0)
                        + len(statement_facts)
                        + len(earnings_facts)
                        + (1 if shares_stats is not None else 0)
                        + len(outstanding_shares)
                        + len(holders)
                        + (1 if splits_dividends is not None else 0)
                        + len(dividend_counts)
                        + len(metric_facts)
                        + len(esg_activities)
                        + (1 if etf_identity is not None else 0)
                        + (1 if mutual_fund_identity is not None else 0)
                        + (1 if index_identity is not None else 0)
                        + len(etf_holdings)
                        + len(mutual_fund_holdings)
                        + len(fund_metric_facts)
                        + len(index_components)
                        + len(index_historical_components)
                    )
                    rejected = len(rejected_rows)
                    total_raw += 1
                    total_valid += rows_valid
                    total_rejected += rejected

                    existing_payload_hash = (
                        load_fundamental_document_payload_hash(ticker, snapshot_date)
                        if refresh_changed_existing
                        else None
                    )
                    if existing_payload_hash == document.row.payload_hash:
                        summary["skipped"].append(ticker)
                        run.record_unit(
                            unit_type="ticker_snapshot",
                            unit_key=unit_key,
                            status="skipped",
                            reason="payload_unchanged",
                            rows_raw=1,
                            rows_valid=rows_valid,
                            rows_rejected=rejected,
                            rows_written=0,
                        )
                        continue

                    landing = await write_fundamental_to_landing(raw, ticker, snapshot_date)
                    if existing_payload_hash is not None:
                        delete_fundamental_snapshot_rows(ticker, snapshot_date)

                    document_write = write_bronze_fundamental_document(document, source_uri=landing.source_uri)
                    identity_write = write_bronze_fundamental_stock_identity(identity, source_uri=landing.source_uri)
                    statement_facts_write = write_bronze_fundamental_statement_facts(
                        statement_facts,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    earnings_facts_write = write_bronze_fundamental_stock_earnings_facts(
                        earnings_facts,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    shares_stats_write = write_bronze_fundamental_stock_shares_stats(
                        shares_stats,
                        source_uri=landing.source_uri,
                    )
                    outstanding_shares_write = write_bronze_fundamental_stock_outstanding_shares(
                        outstanding_shares,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    holders_write = write_bronze_fundamental_stock_holders(
                        holders,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    splits_dividends_write = write_bronze_fundamental_stock_splits_dividends(
                        splits_dividends,
                        source_uri=landing.source_uri,
                    )
                    dividend_counts_write = write_bronze_fundamental_stock_dividend_counts(
                        dividend_counts,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    metric_facts_write = write_bronze_fundamental_stock_metric_facts(
                        metric_facts,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    esg_activities_write = write_bronze_fundamental_stock_esg_activities(
                        esg_activities,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    etf_identity_write = write_bronze_fundamental_etf_identity(
                        etf_identity,
                        source_uri=landing.source_uri,
                    )
                    mutual_fund_identity_write = write_bronze_fundamental_mutual_fund_identity(
                        mutual_fund_identity,
                        source_uri=landing.source_uri,
                    )
                    index_identity_write = write_bronze_fundamental_index_identity(
                        index_identity,
                        source_uri=landing.source_uri,
                    )
                    etf_holdings_write = write_bronze_fundamental_etf_holdings(
                        etf_holdings,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    mutual_fund_holdings_write = write_bronze_fundamental_mutual_fund_holdings(
                        mutual_fund_holdings,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    fund_metric_facts_write = write_bronze_fundamental_fund_metric_facts(
                        fund_metric_facts,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    index_components_write = write_bronze_fundamental_index_components(
                        index_components,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    index_historical_components_write = write_bronze_fundamental_index_historical_components(
                        index_historical_components,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )

                    rows_written = (
                        document_write.rows_written
                        + identity_write.rows_written
                        + statement_facts_write.rows_written
                        + earnings_facts_write.rows_written
                        + shares_stats_write.rows_written
                        + outstanding_shares_write.rows_written
                        + holders_write.rows_written
                        + splits_dividends_write.rows_written
                        + dividend_counts_write.rows_written
                        + metric_facts_write.rows_written
                        + esg_activities_write.rows_written
                        + etf_identity_write.rows_written
                        + mutual_fund_identity_write.rows_written
                        + index_identity_write.rows_written
                        + etf_holdings_write.rows_written
                        + mutual_fund_holdings_write.rows_written
                        + fund_metric_facts_write.rows_written
                        + index_components_write.rows_written
                        + index_historical_components_write.rows_written
                    )
                    total_written += rows_written

                    unit_id = run.record_unit_with_landing(
                        landing,
                        unit_type="ticker_snapshot",
                        unit_key=unit_key,
                        status="completed",
                        reason=_write_reason(
                            document_write.reason,
                            identity_write.reason,
                            statement_facts_write.reason,
                            earnings_facts_write.reason,
                            shares_stats_write.reason,
                            outstanding_shares_write.reason,
                            holders_write.reason,
                            splits_dividends_write.reason,
                            dividend_counts_write.reason,
                            metric_facts_write.reason,
                            esg_activities_write.reason,
                            etf_identity_write.reason,
                            mutual_fund_identity_write.reason,
                            index_identity_write.reason,
                            etf_holdings_write.reason,
                            mutual_fund_holdings_write.reason,
                            fund_metric_facts_write.reason,
                            index_components_write.reason,
                            index_historical_components_write.reason,
                        ),
                        rows_raw=1,
                        rows_valid=rows_valid,
                        rows_rejected=rejected,
                        rows_written=rows_written,
                    )
                    run.record_rejections(
                        _fundamental_rejection_records(
                            run=run,
                            unit_id=unit_id,
                            ticker=ticker,
                            source_uri=landing.source_uri,
                            rejected_rows=rejected_rows,
                        )
                    )
                    summary["tickers"][ticker] = {
                        "family": document.row.instrument_family,
                        "statement_facts": len(statement_facts),
                        "earnings_facts": len(earnings_facts),
                        "shares_stats": shares_stats is not None,
                        "outstanding_shares": len(outstanding_shares),
                        "holders": len(holders),
                        "splits_dividends": splits_dividends is not None,
                        "dividend_counts": len(dividend_counts),
                        "metric_facts": len(metric_facts),
                        "esg_activities": len(esg_activities),
                        "etf_holdings": len(etf_holdings),
                        "mutual_fund_holdings": len(mutual_fund_holdings),
                        "fund_metric_facts": len(fund_metric_facts),
                        "index_components": len(index_components),
                        "index_historical_components": len(index_historical_components),
                        "rows_written": rows_written,
                    }

                except ValidationError as exc:
                    summary["failed"].append(ticker)
                    run.record_unit(
                        unit_type="ticker_snapshot",
                        unit_key=unit_key,
                        status="failed",
                        error=exc,
                        rows_written=0,
                    )
                    run.fail(
                        exc,
                        counters=run.tally.counters(
                            rows_raw=total_raw,
                            rows_valid=total_valid,
                            rows_rejected=total_rejected,
                            rows_written=total_written,
                        ),
                        summary=summary,
                    )
                    raise
                except Exception as exc:
                    log.error("fundamental.ticker_failed", ticker=ticker, error=str(exc))
                    summary["failed"].append(ticker)
                    run.record_unit(
                        unit_type="ticker_snapshot",
                        unit_key=unit_key,
                        status="failed",
                        error=exc,
                        rows_written=0,
                    )

            run.complete(
                status=terminal_status(
                    failed=run.tally.failed,
                    rejected=total_rejected,
                    skipped_all=len(requested_tickers) == 0 or run.tally.skipped == run.tally.total,
                ),
                counters=run.tally.counters(
                    rows_raw=total_raw,
                    rows_valid=total_valid,
                    rows_rejected=total_rejected,
                    rows_written=total_written,
                ),
                summary=summary,
            )
            log.info(
                "fundamental.flow_done",
                snapshot_date=snapshot_date,
                ingested=len(summary["tickers"]),
                skipped=len(summary["skipped"]),
                failed=len(summary["failed"]),
            )
        except Exception as exc:
            if not run.is_terminal:
                run.fail(
                    exc,
                    counters=run.tally.counters(
                        rows_raw=total_raw,
                        rows_valid=total_valid,
                        rows_rejected=total_rejected,
                        rows_written=total_written,
                    ),
                    summary=summary,
                )
            raise

    return summary


def _write_reason(*reasons: str | None) -> str | None:
    reason_set = sorted({reason for reason in reasons if reason})
    return ",".join(reason_set) if reason_set else None


def _fundamental_rejection_records(
    *,
    run: PipelineRunScope,
    unit_id: str,
    ticker: str,
    source_uri: str,
    rejected_rows: list[dict[str, object]],
) -> list[RejectionRecord]:
    """Build capped parser rejection records for one fundamentals document."""
    return [
        run.rejection_record(
            unit_id=unit_id,
            entity_key={
                "ticker": ticker,
                "section": row.get("section"),
                "earnings_section": row.get("earnings_section"),
                "holder_type": row.get("holder_type"),
                "statement_type": row.get("statement_type"),
                "period_type": row.get("period_type"),
                "period_key": row.get("period_key"),
                "metric_name": row.get("metric_name"),
            },
            source_uri=source_uri,
            raw_fragment=row,
            reason=str(row.get("reason", "parse_rejected")),
        )
        for row in rejected_rows[:_REJECTION_SAMPLE_LIMIT_PER_TICKER]
    ]


if __name__ == "__main__":
    asyncio.run(fundamental_flow(tickers=["AAPL.US"], limit=1))
