"""Domain service for fundamentals ingestion."""

import asyncio
from collections.abc import AsyncIterator
from datetime import date
from typing import cast

import structlog
from pydantic import ValidationError

from core.http.base import ProviderRateLimitError
from core.ingestion import (
    LandingWrite,
    PipelineRunScope,
    PipelineRunTracker,
    RejectionRecord,
    RunStatus,
    terminal_status,
)
from core.orchestration.events import publish_prefect_ingestion_summary
from domains.fundamental.contracts import FundamentalRefreshRequest, FundamentalRefreshResult
from domains.fundamental.tasks import (
    delete_fundamental_snapshot_rows,
    fetch_fundamental_instrument,
    fetch_fundamental_provider_exchange_codes,
    fundamental_document_already_ingested,
    load_fundamental_document_payload_hash,
    load_fundamental_from_landing,
    load_fundamental_instrument_selection,
    parse_fundamental_stock,
    write_fundamental_deferred_coverage,
    write_fundamental_to_landing,
)
from domains.fundamental.writers import write_fundamental_bronze_slices
from providers.eodhd.identifiers import EODHDInstrumentRef, eodhd_instrument_key
from providers.eodhd.models import FundamentalRaw

from .assets import record_fundamental_bronze_materialization

log = structlog.get_logger(__name__)
_REJECTION_SAMPLE_LIMIT_PER_INSTRUMENT = 100


async def run_fundamental_refresh(request: FundamentalRefreshRequest) -> FundamentalRefreshResult:
    """Ingest fundamentals for explicit provider instruments or latest provider instruments.

    Automatic provider_instrument_code selection reads all latest EODHD provider instruments from
    ``silver.int_fundamental_ingestion_universe``; run instrument and
    fundamental dbt builds before auto-selecting instruments. The flow lands every
    fundamentals document for replay, writes document metadata, and extracts only
    the family-specific slices that active marts currently model.
    ``snapshot_date`` and ``ingestion_batch_date`` are the bronze partition key
    ``(snapshot_date, provider_instrument_code)``. Use ``ingestion_batch_date`` to pin a multi-day
    backfill campaign. With ``continue_ingestion_batch=True`` and no explicit
    date, the flow reuses ``MAX(snapshot_date)`` from ``bronze.fundamental_document``
    so a later run does not treat every provider_instrument_code as pending again. Manual runs
    without those flags still default to today. By default, already-ingested
    provider_instrument_code snapshots are skipped before fetching to avoid spending provider
    credits. Set ``refresh_existing=True`` to fetch existing provider_instrument_code snapshots,
    compare payload hashes, and replace same-day Bronze rows only when the
    provider document changed. Passing ``skip_existing=False`` uses the same
    changed-payload refresh behavior. Set ``replay_landing=True`` to load
    previously landed JSON documents instead of calling the provider.
    ``batch_size`` controls concurrent provider fetches only; landing and
    Bronze writes stay sequential. ``max_provider_credits`` can cap provider
    calls for fundamentals backfills where each EODHD call costs 10 credits.
    If the provider returns HTTP 429, the flow stops after the active fetch
    batch and records unsubmitted instruments as deferred so the same batch date can
    continue after the provider quota resets.
    """
    from domains.fundamental.tasks import resolve_fundamental_snapshot_date_task

    resolved_batch = resolve_fundamental_snapshot_date_task(
        snapshot_date=request.snapshot_date,
        ingestion_batch_date=request.ingestion_batch_date,
        continue_ingestion_batch=request.continue_ingestion_batch,
    )
    snapshot_date = date.fromisoformat(str(resolved_batch["snapshot_date"]))
    snapshot_date_source = str(resolved_batch["source"])
    if snapshot_date_source == "bronze_latest":
        log.info(
            "fundamental.ingestion_batch_continued",
            snapshot_date=snapshot_date.isoformat(),
            source=snapshot_date_source,
        )

    refresh_changed_existing = request.refresh_existing or not request.skip_existing
    auto_selection_anti_joined = False
    if request.provider_instruments:
        requested_instruments = _coerce_instrument_refs(request.provider_instruments)
    elif request.provider_exchange_codes == []:
        requested_instruments = []
    else:
        selected_provider_exchange_codes = (
            fetch_fundamental_provider_exchange_codes()
            if request.provider_exchange_codes is None
            else request.provider_exchange_codes
        )
        skip_completed = request.skip_existing and not refresh_changed_existing
        instrument_selection = load_fundamental_instrument_selection(
            selected_provider_exchange_codes,
            request.limit,
            snapshot_date=snapshot_date,
            skip_completed=skip_completed,
        )
        requested_instruments = instrument_selection.instruments
        auto_selection_anti_joined = instrument_selection.completion_filter_applied
    fetch_batch_size = max(1, int(request.batch_size))
    fetch_batch_delay = max(0.0, float(request.provider_batch_delay_seconds))
    provider_credit_cost = max(1, int(request.provider_credits_per_call))

    summary: dict = {
        "snapshot_date": snapshot_date.isoformat(),
        "snapshot_date_source": snapshot_date_source,
        "instruments": {},
        "skipped": [],
        "failed": [],
        "deferred": [],
        "provider_quota_exhausted": False,
        "auto_selection_anti_joined": auto_selection_anti_joined,
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
            "provider_instruments": request.provider_instruments,
            "snapshot_date": snapshot_date.isoformat(),
            "ingestion_batch_date": request.ingestion_batch_date.isoformat() if request.ingestion_batch_date else None,
            "continue_ingestion_batch": request.continue_ingestion_batch,
            "snapshot_date_source": snapshot_date_source,
            "provider_exchange_codes": request.provider_exchange_codes,
            "limit": request.limit,
            "skip_existing": request.skip_existing,
            "refresh_existing": request.refresh_existing,
            "refresh_changed_existing": refresh_changed_existing,
            "replay_landing": request.replay_landing,
            "landing_source_uris_by_instrument": request.landing_source_uris_by_instrument,
            "batch_size": fetch_batch_size,
            "provider_batch_delay_seconds": fetch_batch_delay,
            "provider_credits_per_call": provider_credit_cost,
            "max_provider_credits": request.max_provider_credits,
        },
        target_window_start=snapshot_date,
        target_window_end=snapshot_date,
    ) as run:
        run_id = str(run.run_id)
        try:
            pending_instruments: list[EODHDInstrumentRef] = []
            for instrument in requested_instruments:
                instrument_key = _instrument_key(instrument)
                unit_key: dict[str, object] = {
                    "provider_exchange_code": instrument.provider_exchange_code,
                    "provider_instrument_code": instrument.provider_instrument_code,
                    "snapshot_date": snapshot_date.isoformat(),
                }
                if (
                    not auto_selection_anti_joined
                    and request.skip_existing
                    and not refresh_changed_existing
                    and fundamental_document_already_ingested(
                        instrument.provider_exchange_code,
                        instrument.provider_instrument_code,
                        snapshot_date,
                    )
                ):
                    summary["skipped"].append(instrument_key)
                    run.record_unit(
                        unit_type="instrument_snapshot",
                        unit_key=unit_key,
                        status="skipped",
                        reason="already_ingested",
                        rows_written=0,
                    )
                    continue
                pending_instruments.append(instrument)

            if not request.replay_landing and request.max_provider_credits is not None:
                max_provider_calls = max(0, int(request.max_provider_credits) // provider_credit_cost)
                skipped_for_budget = pending_instruments[max_provider_calls:]
                pending_instruments = pending_instruments[:max_provider_calls]
                if skipped_for_budget:
                    write_fundamental_deferred_coverage(
                        run_id=str(run.run_id),
                        instruments=skipped_for_budget,
                        snapshot_date=snapshot_date,
                        reason="credit_budget_exhausted",
                    )
                for instrument in skipped_for_budget:
                    instrument_key = _instrument_key(instrument)
                    summary["skipped"].append(instrument_key)
                    run.record_unit(
                        unit_type="instrument_snapshot",
                        unit_key={
                            "provider_exchange_code": instrument.provider_exchange_code,
                            "provider_instrument_code": instrument.provider_instrument_code,
                            "snapshot_date": snapshot_date.isoformat(),
                        },
                        status="skipped",
                        reason="credit_budget_exhausted",
                        rows_written=0,
                    )

            processed_instruments: set[str] = set()
            stop_after_fetch_batch = False
            async for instrument, result, is_batch_end in _fundamental_raw_results(
                pending_instruments,
                snapshot_date=snapshot_date,
                replay_landing=request.replay_landing,
                landing_source_uris_by_instrument=request.landing_source_uris_by_instrument,
                fetch_batch_size=fetch_batch_size,
                fetch_batch_delay=fetch_batch_delay,
            ):
                instrument_key = _instrument_key(instrument)
                provider_exchange_code = instrument.provider_exchange_code
                provider_instrument_code = instrument.provider_instrument_code
                processed_instruments.add(instrument_key)
                unit_key = {
                    "provider_exchange_code": provider_exchange_code,
                    "provider_instrument_code": provider_instrument_code,
                    "snapshot_date": snapshot_date.isoformat(),
                }
                if isinstance(result, ProviderRateLimitError):
                    log.warning(
                        "fundamental.provider_quota_exhausted",
                        provider_exchange_code=provider_exchange_code,
                        provider_instrument_code=provider_instrument_code,
                        retry_after=result.retry_after,
                    )
                    summary["provider_quota_exhausted"] = True
                    summary["failed"].append(instrument_key)
                    summary["deferred"].append(instrument_key)
                    run.record_unit(
                        unit_type="instrument_snapshot",
                        unit_key=unit_key,
                        status="failed",
                        reason="provider_rate_limited",
                        error=result,
                        rows_written=0,
                    )
                    stop_after_fetch_batch = True
                    if is_batch_end:
                        _defer_provider_rate_limited_instruments(
                            run=run,
                            summary=summary,
                            instruments=_unprocessed_instruments(pending_instruments, processed_instruments),
                            snapshot_date=snapshot_date,
                        )
                        break
                    continue
                if isinstance(result, ValidationError):
                    summary["failed"].append(instrument_key)
                    run.record_unit(
                        unit_type="instrument_snapshot",
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
                    log.error(
                        "fundamental.instrument_failed",
                        provider_exchange_code=provider_exchange_code,
                        provider_instrument_code=provider_instrument_code,
                        error=str(result),
                    )
                    summary["failed"].append(instrument_key)
                    run.record_unit(
                        unit_type="instrument_snapshot",
                        unit_key=unit_key,
                        status="failed",
                        error=result,
                        rows_written=0,
                    )
                    continue

                try:
                    if request.replay_landing:
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
                        metric_facts,
                        etf_identity,
                        mutual_fund_identity,
                        index_identity,
                        etf_holdings,
                        mutual_fund_holdings,
                        fund_metric_facts,
                        index_components,
                        rejected_rows,
                    ) = parse_fundamental_stock(raw, provider_exchange_code, provider_instrument_code, snapshot_date)
                    rows_valid = (
                        1
                        + (1 if identity is not None else 0)
                        + len(statement_facts)
                        + len(earnings_facts)
                        + (1 if shares_stats is not None else 0)
                        + len(outstanding_shares)
                        + len(holders)
                        + len(insider_transactions)
                        + len(metric_facts)
                        + (1 if etf_identity is not None else 0)
                        + (1 if mutual_fund_identity is not None else 0)
                        + (1 if index_identity is not None else 0)
                        + len(etf_holdings)
                        + len(mutual_fund_holdings)
                        + len(fund_metric_facts)
                        + len(index_components)
                    )
                    rejected = len(rejected_rows)
                    total_raw += 1
                    total_valid += rows_valid
                    total_rejected += rejected

                    existing_payload_hash = (
                        load_fundamental_document_payload_hash(
                            provider_exchange_code,
                            provider_instrument_code,
                            snapshot_date,
                        )
                        if refresh_changed_existing
                        else None
                    )
                    if existing_payload_hash == document.row.payload_hash:
                        summary["skipped"].append(instrument_key)
                        run.record_unit(
                            unit_type="instrument_snapshot",
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
                        landing = await write_fundamental_to_landing(
                            raw,
                            provider_exchange_code,
                            provider_instrument_code,
                            snapshot_date,
                        )
                    if existing_payload_hash is not None:
                        delete_fundamental_snapshot_rows(
                            provider_exchange_code,
                            provider_instrument_code,
                            snapshot_date,
                        )

                    bronze_write = write_fundamental_bronze_slices(
                        document=document,
                        identity=identity,
                        statement_facts=statement_facts,
                        earnings_facts=earnings_facts,
                        shares_stats=shares_stats,
                        outstanding_shares=outstanding_shares,
                        holders=holders,
                        insider_transactions=insider_transactions,
                        metric_facts=metric_facts,
                        etf_identity=etf_identity,
                        mutual_fund_identity=mutual_fund_identity,
                        index_identity=index_identity,
                        etf_holdings=etf_holdings,
                        mutual_fund_holdings=mutual_fund_holdings,
                        fund_metric_facts=fund_metric_facts,
                        index_components=index_components,
                        provider_instrument_code=provider_instrument_code,
                        snapshot_date=snapshot_date,
                        source_uri=landing.source_uri,
                    )
                    rows_written = bronze_write.rows_written
                    total_written += rows_written

                    unit_id = run.record_unit_with_landing(
                        landing,
                        unit_type="instrument_snapshot",
                        unit_key=unit_key,
                        status="completed",
                        reason=bronze_write.reason,
                        rows_raw=1,
                        rows_valid=rows_valid,
                        rows_rejected=rejected,
                        rows_written=rows_written,
                    )
                    run.record_rejections(
                        _fundamental_rejection_records(
                            run=run,
                            unit_id=unit_id,
                            provider_exchange_code=provider_exchange_code,
                            provider_instrument_code=provider_instrument_code,
                            source_uri=landing.source_uri,
                            rejected_rows=rejected_rows,
                        )
                    )
                    summary["instruments"][instrument_key] = {
                        "family": document.row.instrument_family,
                        "statement_facts": len(statement_facts),
                        "earnings_facts": len(earnings_facts),
                        "shares_stats": shares_stats is not None,
                        "outstanding_shares": len(outstanding_shares),
                        "holders": len(holders),
                        "insider_transactions": len(insider_transactions),
                        "metric_facts": len(metric_facts),
                        "etf_holdings": len(etf_holdings),
                        "mutual_fund_holdings": len(mutual_fund_holdings),
                        "fund_metric_facts": len(fund_metric_facts),
                        "index_components": len(index_components),
                        "rows_written": rows_written,
                    }

                except ValidationError as exc:
                    summary["failed"].append(instrument_key)
                    run.record_unit(
                        unit_type="instrument_snapshot",
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
                    log.error(
                        "fundamental.instrument_failed",
                        provider_exchange_code=provider_exchange_code,
                        provider_instrument_code=provider_instrument_code,
                        error=str(exc),
                    )
                    summary["failed"].append(instrument_key)
                    run.record_unit(
                        unit_type="instrument_snapshot",
                        unit_key=unit_key,
                        status="failed",
                        error=exc,
                        rows_written=0,
                    )

                if stop_after_fetch_batch and is_batch_end:
                    _defer_provider_rate_limited_instruments(
                        run=run,
                        summary=summary,
                        instruments=_unprocessed_instruments(pending_instruments, processed_instruments),
                        snapshot_date=snapshot_date,
                    )
                    break

            run_status = terminal_status(
                failed=run.tally.failed,
                rejected=total_rejected,
                skipped_all=len(requested_instruments) == 0 or run.tally.skipped == run.tally.total,
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
            if total_written:
                record_fundamental_bronze_materialization(
                    app_run_id=run.run_id,
                    snapshot_date=snapshot_date.isoformat(),
                    provider="eodhd",
                    rows_written=total_written,
                    instruments=len(summary["instruments"]),
                )
            await publish_prefect_ingestion_summary(
                flow_name="fundamental-quarterly",
                domain="fundamental",
                app_run_id=run.run_id,
                status=run_status,
                summary=summary,
            )
            log.info(
                "fundamental.flow_done",
                snapshot_date=snapshot_date,
                ingested=len(summary["instruments"]),
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
            await publish_prefect_ingestion_summary(
                flow_name="fundamental-quarterly",
                domain="fundamental",
                app_run_id=run.run_id,
                status="failed",
                summary=summary,
            )
            raise

    if run_id is None or run_status is None:
        msg = "Fundamentals refresh did not record a run result."
        raise RuntimeError(msg)
    return FundamentalRefreshResult(run_id=run_id, status=run_status, summary=summary)


async def _fundamental_raw_results(
    instruments: list[EODHDInstrumentRef],
    *,
    snapshot_date: date,
    replay_landing: bool,
    landing_source_uris_by_instrument: dict[str, str] | None,
    fetch_batch_size: int,
    fetch_batch_delay: float,
) -> AsyncIterator[
    tuple[EODHDInstrumentRef, FundamentalRaw | tuple[FundamentalRaw, LandingWrite] | BaseException, bool]
]:
    """Yield fetched or replayed fundamentals payloads one batch at a time."""
    for i in range(0, len(instruments), fetch_batch_size):
        batch_instruments = instruments[i : i + fetch_batch_size]
        if replay_landing:
            raw_results = await asyncio.gather(
                *[
                    load_fundamental_from_landing(
                        instrument.provider_exchange_code,
                        instrument.provider_instrument_code,
                        snapshot_date,
                        source_uri=(landing_source_uris_by_instrument or {}).get(_instrument_key(instrument)),
                    )
                    for instrument in batch_instruments
                ],
                return_exceptions=True,
            )
        else:
            raw_results = await asyncio.gather(
                *[
                    fetch_fundamental_instrument(
                        instrument.provider_exchange_code,
                        instrument.provider_instrument_code,
                    )
                    for instrument in batch_instruments
                ],
                return_exceptions=True,
            )

        last_index = len(batch_instruments) - 1
        for batch_index, (instrument, result) in enumerate(zip(batch_instruments, raw_results, strict=True)):
            yield instrument, result, batch_index == last_index

        if not replay_landing and fetch_batch_delay > 0 and i + fetch_batch_size < len(instruments):
            await asyncio.sleep(fetch_batch_delay)


def _coerce_instrument_refs(values: list[dict[str, str]]) -> list[EODHDInstrumentRef]:
    """Coerce flow parameter dictionaries into provider instrument refs."""
    refs: list[EODHDInstrumentRef] = []
    for value in values:
        refs.append(
            EODHDInstrumentRef(
                provider_exchange_code=str(value["provider_exchange_code"]),
                provider_instrument_code=str(value["provider_instrument_code"]),
            )
        )
    return refs


def _instrument_key(instrument: EODHDInstrumentRef) -> str:
    """Return a stable display key for one provider instrument ref."""
    return eodhd_instrument_key(
        provider_exchange_code=instrument.provider_exchange_code,
        provider_instrument_code=instrument.provider_instrument_code,
    )


def _unprocessed_instruments(
    instruments: list[EODHDInstrumentRef],
    processed_instruments: set[str],
) -> list[EODHDInstrumentRef]:
    """Return pending instruments that have not had a fetch result processed."""
    return [instrument for instrument in instruments if _instrument_key(instrument) not in processed_instruments]


def _defer_provider_rate_limited_instruments(
    *,
    run: PipelineRunScope,
    summary: dict,
    instruments: list[EODHDInstrumentRef],
    snapshot_date: date,
) -> None:
    """Record unsubmitted fundamentals work that should resume after quota reset."""
    if not instruments:
        return
    write_fundamental_deferred_coverage(
        run_id=str(run.run_id),
        instruments=instruments,
        snapshot_date=snapshot_date,
        reason="provider_rate_limited",
    )
    for instrument in instruments:
        instrument_key = _instrument_key(instrument)
        summary["skipped"].append(instrument_key)
        summary["deferred"].append(instrument_key)
        run.record_unit(
            unit_type="instrument_snapshot",
            unit_key={
                "provider_exchange_code": instrument.provider_exchange_code,
                "provider_instrument_code": instrument.provider_instrument_code,
                "snapshot_date": snapshot_date.isoformat(),
            },
            status="skipped",
            reason="provider_rate_limited",
            rows_written=0,
        )


def _fundamental_rejection_records(
    *,
    run: PipelineRunScope,
    unit_id: str,
    provider_exchange_code: str,
    provider_instrument_code: str,
    source_uri: str,
    rejected_rows: list[dict[str, object]],
) -> list[RejectionRecord]:
    """Build capped parser rejection records for one fundamentals document."""
    return [
        run.rejection_record(
            unit_id=unit_id,
            entity_key={
                "provider_exchange_code": provider_exchange_code,
                "provider_instrument_code": provider_instrument_code,
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
        for row in rejected_rows[:_REJECTION_SAMPLE_LIMIT_PER_INSTRUMENT]
    ]
