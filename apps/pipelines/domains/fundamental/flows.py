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
    write_bronze_fundamental_statement_facts,
    write_bronze_fundamental_stock_identity,
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
                    document, identity, facts, rejected_rows = parse_fundamental_stock(raw, ticker, snapshot_date)
                    rows_valid = 1 + (1 if identity is not None else 0) + len(facts)
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
                    facts_write = write_bronze_fundamental_statement_facts(
                        facts,
                        ticker=ticker,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )

                    rows_written = document_write.rows_written + identity_write.rows_written + facts_write.rows_written
                    total_written += rows_written

                    unit_id = run.record_unit_with_landing(
                        landing,
                        unit_type="ticker_snapshot",
                        unit_key=unit_key,
                        status="completed",
                        reason=_write_reason(
                            document_write.reason,
                            identity_write.reason,
                            facts_write.reason,
                        ),
                        rows_raw=1,
                        rows_valid=rows_valid,
                        rows_rejected=rejected,
                        rows_written=rows_written,
                    )
                    run.record_rejections(
                        _statement_rejection_records(
                            run=run,
                            unit_id=unit_id,
                            ticker=ticker,
                            source_uri=landing.source_uri,
                            rejected_rows=rejected_rows,
                        )
                    )
                    summary["tickers"][ticker] = {
                        "family": document.row.instrument_family,
                        "statement_facts": len(facts),
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


def _statement_rejection_records(
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
