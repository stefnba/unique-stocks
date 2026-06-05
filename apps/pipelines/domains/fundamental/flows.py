"""Fundamentals ingestion flow."""

import asyncio
from collections.abc import AsyncIterator
from datetime import date
from typing import cast

import structlog
from prefect import flow
from pydantic import ValidationError

from core.clients.http.base import ProviderRateLimitError
from core.ingestion import (
    LandingWrite,
    PipelineRunScope,
    PipelineRunTracker,
    RejectionRecord,
    RunStatus,
    terminal_status,
)
from core.transforms import run_dbt_build_after_ingestion
from domains.fundamental.tasks import (
    delete_fundamental_snapshot_rows,
    fetch_fundamental_provider_exchange_codes,
    fetch_fundamental_ticker,
    fundamental_document_already_ingested,
    load_fundamental_document_payload_hash,
    load_fundamental_from_landing,
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
    write_bronze_fundamental_stock_insider_transactions,
    write_bronze_fundamental_stock_metric_facts,
    write_bronze_fundamental_stock_outstanding_shares,
    write_bronze_fundamental_stock_shares_stats,
    write_bronze_fundamental_stock_splits_dividends,
    write_fundamental_deferred_coverage,
    write_fundamental_to_landing,
)
from providers.eodhd.models import FundamentalRaw

log = structlog.get_logger(__name__)
_REJECTION_SAMPLE_LIMIT_PER_TICKER = 100


@flow(
    name="fundamental-quarterly",
    description=(
        "Ingest EODHD fundamental JSON per ticker: identity, statements, metrics, holders, "
        "insider transactions, dividends, and more. Writes S3 landing + multiple bronze.fundamental_* tables."
    ),
)
async def fundamental_flow(
    tickers: list[str] | None = None,
    snapshot_date: date | None = None,
    ingestion_batch_date: date | None = None,
    continue_ingestion_batch: bool = False,
    provider_exchange_codes: list[str] | None = None,
    limit: int | None = None,
    skip_existing: bool = True,
    refresh_existing: bool = False,
    replay_landing: bool = False,
    landing_source_uris_by_ticker: dict[str, str] | None = None,
    batch_size: int = 1,
    provider_batch_delay_seconds: float = 0.0,
    provider_credits_per_call: int = 10,
    max_provider_credits: int | None = None,
    run_dbt_build: bool = False,
) -> dict[str, object]:
    """Ingest fundamentals for explicit tickers or latest stock instruments.

    This first implementation supports stock fundamentals. Non-stock documents
    still produce a document metadata row, but family-specific ETF/fund/index
    extraction is intentionally left for separate parser slices with fixtures.
    ``snapshot_date`` and ``ingestion_batch_date`` are the bronze partition key
    ``(snapshot_date, ticker)``. Use ``ingestion_batch_date`` to pin a multi-day
    backfill campaign. With ``continue_ingestion_batch=True`` and no explicit
    date, the flow reuses ``MAX(snapshot_date)`` from ``bronze.fundamental_document``
    so a later run does not treat every ticker as pending again. Manual runs
    without those flags still default to today. By default, already-ingested
    ticker snapshots are skipped before fetching to avoid spending provider
    credits. Set ``refresh_existing=True`` to fetch existing ticker snapshots,
    compare payload hashes, and replace same-day Bronze rows only when the
    provider document changed. Passing ``skip_existing=False`` uses the same
    changed-payload refresh behavior. Set ``replay_landing=True`` to load
    previously landed JSON documents instead of calling the provider.
    ``batch_size`` controls concurrent provider fetches only; landing and
    Bronze writes stay sequential. ``max_provider_credits`` can cap provider
    calls for fundamentals backfills where each EODHD call costs 10 credits.
    If the provider returns HTTP 429, the flow stops after the active fetch
    batch and records unsubmitted tickers as deferred so the same batch date can
    continue after the provider quota resets. Set ``run_dbt_build=True`` to
    launch ``dbt-build/fundamental-build`` after a clean ingestion audit status.
    """
    from domains.fundamental.tasks import resolve_fundamental_snapshot_date_task

    resolved_batch = resolve_fundamental_snapshot_date_task(
        snapshot_date=snapshot_date,
        ingestion_batch_date=ingestion_batch_date,
        continue_ingestion_batch=continue_ingestion_batch,
    )
    snapshot_date = date.fromisoformat(str(resolved_batch["snapshot_date"]))
    snapshot_date_source = str(resolved_batch["source"])
    if snapshot_date_source == "bronze_latest":
        log.info(
            "fundamental.ingestion_batch_continued",
            snapshot_date=snapshot_date.isoformat(),
            source=snapshot_date_source,
        )

    if tickers:
        requested_tickers = tickers
    else:
        selected_provider_exchange_codes = provider_exchange_codes or fetch_fundamental_provider_exchange_codes()
        requested_tickers = load_fundamental_stock_tickers(selected_provider_exchange_codes, limit)
    refresh_changed_existing = refresh_existing or not skip_existing
    fetch_batch_size = max(1, int(batch_size))
    fetch_batch_delay = max(0.0, float(provider_batch_delay_seconds))
    provider_credit_cost = max(1, int(provider_credits_per_call))

    summary: dict = {
        "snapshot_date": snapshot_date.isoformat(),
        "snapshot_date_source": snapshot_date_source,
        "tickers": {},
        "skipped": [],
        "failed": [],
        "deferred": [],
        "provider_quota_exhausted": False,
    }
    total_raw = 0
    total_valid = 0
    total_rejected = 0
    total_written = 0

    tracker = PipelineRunTracker()
    run_id: str | None = None
    run_status: RunStatus | None = None
    with tracker.track_run(
        flow_name="fundamental-quarterly",
        domain="fundamental",
        run_kind="snapshot",
        provider="eodhd",
        parameters={
            "tickers": tickers,
            "snapshot_date": snapshot_date.isoformat(),
            "ingestion_batch_date": ingestion_batch_date.isoformat() if ingestion_batch_date else None,
            "continue_ingestion_batch": continue_ingestion_batch,
            "snapshot_date_source": snapshot_date_source,
            "provider_exchange_codes": provider_exchange_codes,
            "limit": limit,
            "skip_existing": skip_existing,
            "refresh_existing": refresh_existing,
            "refresh_changed_existing": refresh_changed_existing,
            "replay_landing": replay_landing,
            "landing_source_uris_by_ticker": landing_source_uris_by_ticker,
            "batch_size": fetch_batch_size,
            "provider_batch_delay_seconds": fetch_batch_delay,
            "provider_credits_per_call": provider_credit_cost,
            "max_provider_credits": max_provider_credits,
            "run_dbt_build": run_dbt_build,
        },
        target_window_start=snapshot_date,
        target_window_end=snapshot_date,
    ) as run:
        run_id = str(run.run_id)
        try:
            pending_tickers: list[str] = []
            for ticker in requested_tickers:
                unit_key: dict[str, object] = {"ticker": ticker, "snapshot_date": snapshot_date.isoformat()}
                if (
                    skip_existing
                    and not refresh_changed_existing
                    and fundamental_document_already_ingested(ticker, snapshot_date)
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
                pending_tickers.append(ticker)

            if not replay_landing and max_provider_credits is not None:
                max_provider_calls = max(0, int(max_provider_credits) // provider_credit_cost)
                skipped_for_budget = pending_tickers[max_provider_calls:]
                pending_tickers = pending_tickers[:max_provider_calls]
                if skipped_for_budget:
                    write_fundamental_deferred_coverage(
                        run_id=str(run.run_id),
                        tickers=skipped_for_budget,
                        snapshot_date=snapshot_date,
                        reason="credit_budget_exhausted",
                    )
                for ticker in skipped_for_budget:
                    summary["skipped"].append(ticker)
                    run.record_unit(
                        unit_type="ticker_snapshot",
                        unit_key={"ticker": ticker, "snapshot_date": snapshot_date.isoformat()},
                        status="skipped",
                        reason="credit_budget_exhausted",
                        rows_written=0,
                    )

            processed_tickers: set[str] = set()
            stop_after_fetch_batch = False
            async for ticker, result, is_batch_end in _fundamental_raw_results(
                pending_tickers,
                snapshot_date=snapshot_date,
                replay_landing=replay_landing,
                landing_source_uris_by_ticker=landing_source_uris_by_ticker,
                fetch_batch_size=fetch_batch_size,
                fetch_batch_delay=fetch_batch_delay,
            ):
                processed_tickers.add(ticker)
                unit_key = {"ticker": ticker, "snapshot_date": snapshot_date.isoformat()}
                if isinstance(result, ProviderRateLimitError):
                    log.warning(
                        "fundamental.provider_quota_exhausted",
                        ticker=ticker,
                        retry_after=result.retry_after,
                    )
                    summary["provider_quota_exhausted"] = True
                    summary["failed"].append(ticker)
                    summary["deferred"].append(ticker)
                    run.record_unit(
                        unit_type="ticker_snapshot",
                        unit_key=unit_key,
                        status="failed",
                        reason="provider_rate_limited",
                        error=result,
                        rows_written=0,
                    )
                    stop_after_fetch_batch = True
                    if is_batch_end:
                        _defer_provider_rate_limited_tickers(
                            run=run,
                            summary=summary,
                            tickers=_unprocessed_tickers(pending_tickers, processed_tickers),
                            snapshot_date=snapshot_date,
                        )
                        break
                    continue
                if isinstance(result, ValidationError):
                    summary["failed"].append(ticker)
                    run.record_unit(
                        unit_type="ticker_snapshot",
                        unit_key=unit_key,
                        status="failed",
                        error=result,
                        rows_written=0,
                    )
                    run.fail(
                        result,
                        counters=run.tally.counters(
                            rows_raw=total_raw,
                            rows_valid=total_valid,
                            rows_rejected=total_rejected,
                            rows_written=total_written,
                        ),
                        summary=summary,
                    )
                    raise result
                if isinstance(result, BaseException):
                    log.error("fundamental.ticker_failed", ticker=ticker, error=str(result))
                    summary["failed"].append(ticker)
                    run.record_unit(
                        unit_type="ticker_snapshot",
                        unit_key=unit_key,
                        status="failed",
                        error=result,
                        rows_written=0,
                    )
                    continue

                try:
                    if replay_landing:
                        raw, landing = cast(tuple[FundamentalRaw, LandingWrite], result)
                    else:
                        raw = cast(FundamentalRaw, result)
                        landing = None
                    (
                        document,
                        identity,
                        statement_facts,
                        earnings_facts,
                        shares_stats,
                        outstanding_shares,
                        holders,
                        insider_transactions,
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
                        + len(insider_transactions)
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

                    if landing is None:
                        landing = await write_fundamental_to_landing(raw, ticker, snapshot_date)
                    if existing_payload_hash is not None:
                        delete_fundamental_snapshot_rows(ticker, snapshot_date)

                    document_write = write_bronze_fundamental_document(document, source_uri=landing.source_uri)
                    identity_write = write_bronze_fundamental_stock_identity(
                        identity,
                        source_uri=landing.source_uri,
                        ticker=ticker,
                    )
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
                        ticker=ticker,
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
                    insider_transactions_write = write_bronze_fundamental_stock_insider_transactions(
                        insider_transactions,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    splits_dividends_write = write_bronze_fundamental_stock_splits_dividends(
                        splits_dividends,
                        source_uri=landing.source_uri,
                        ticker=ticker,
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
                        ticker=ticker,
                    )
                    mutual_fund_identity_write = write_bronze_fundamental_mutual_fund_identity(
                        mutual_fund_identity,
                        source_uri=landing.source_uri,
                        ticker=ticker,
                    )
                    index_identity_write = write_bronze_fundamental_index_identity(
                        index_identity,
                        source_uri=landing.source_uri,
                        ticker=ticker,
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
                        + insider_transactions_write.rows_written
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
                            insider_transactions_write.reason,
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
                        "insider_transactions": len(insider_transactions),
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

                if stop_after_fetch_batch and is_batch_end:
                    _defer_provider_rate_limited_tickers(
                        run=run,
                        summary=summary,
                        tickers=_unprocessed_tickers(pending_tickers, processed_tickers),
                        snapshot_date=snapshot_date,
                    )
                    break

            run_status = terminal_status(
                failed=run.tally.failed,
                rejected=total_rejected,
                skipped_all=len(requested_tickers) == 0 or run.tally.skipped == run.tally.total,
            )
            run.complete(
                status=run_status,
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

    if run_dbt_build and run_id is not None and run_status is not None:
        summary["dbt_build"] = await run_dbt_build_after_ingestion(
            enabled=run_dbt_build,
            build="fundamental-build",
            upstream_status=run_status,
            parent_run_id=run_id,
        )
    return summary


async def _fundamental_raw_results(
    tickers: list[str],
    *,
    snapshot_date: date,
    replay_landing: bool,
    landing_source_uris_by_ticker: dict[str, str] | None,
    fetch_batch_size: int,
    fetch_batch_delay: float,
) -> AsyncIterator[tuple[str, FundamentalRaw | tuple[FundamentalRaw, LandingWrite] | BaseException, bool]]:
    """Yield fetched or replayed fundamentals payloads one batch at a time."""
    for i in range(0, len(tickers), fetch_batch_size):
        batch_tickers = tickers[i : i + fetch_batch_size]
        if replay_landing:
            raw_results = await asyncio.gather(
                *[
                    load_fundamental_from_landing(
                        ticker,
                        snapshot_date,
                        source_uri=(landing_source_uris_by_ticker or {}).get(ticker),
                    )
                    for ticker in batch_tickers
                ],
                return_exceptions=True,
            )
        else:
            raw_results = await asyncio.gather(
                *[fetch_fundamental_ticker(ticker) for ticker in batch_tickers],
                return_exceptions=True,
            )

        last_index = len(batch_tickers) - 1
        for batch_index, (ticker, result) in enumerate(zip(batch_tickers, raw_results, strict=True)):
            yield ticker, result, batch_index == last_index

        if not replay_landing and fetch_batch_delay > 0 and i + fetch_batch_size < len(tickers):
            await asyncio.sleep(fetch_batch_delay)


def _write_reason(*reasons: str | None) -> str | None:
    """Combine non-empty Bronze write reasons into a stable unit reason string."""
    reason_set = sorted({reason for reason in reasons if reason})
    return ",".join(reason_set) if reason_set else None


def _unprocessed_tickers(tickers: list[str], processed_tickers: set[str]) -> list[str]:
    """Return pending tickers that have not had a fetch result processed."""
    return [ticker for ticker in tickers if ticker not in processed_tickers]


def _defer_provider_rate_limited_tickers(
    *,
    run: PipelineRunScope,
    summary: dict,
    tickers: list[str],
    snapshot_date: date,
) -> None:
    """Record unsubmitted fundamentals work that should resume after quota reset."""
    if not tickers:
        return
    write_fundamental_deferred_coverage(
        run_id=str(run.run_id),
        tickers=tickers,
        snapshot_date=snapshot_date,
        reason="provider_rate_limited",
    )
    for ticker in tickers:
        summary["skipped"].append(ticker)
        summary["deferred"].append(ticker)
        run.record_unit(
            unit_type="ticker_snapshot",
            unit_key={"ticker": ticker, "snapshot_date": snapshot_date.isoformat()},
            status="skipped",
            reason="provider_rate_limited",
            rows_written=0,
        )


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
