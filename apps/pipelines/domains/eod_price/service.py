"""EOD price domain service.

Ingests end-of-day OHLCV price for all exchange using the provider bulk
endpoint — one API call per exchange per date, not one per instrument.

Schedule: trigger per exchange after that exchange's market close.
For simplicity, a single daily run at 22:00 UTC catches all exchange
that closed by then (US at ~21:00 UTC, Europe by ~18:00 UTC).
"""

import asyncio
import uuid
from dataclasses import dataclass
from datetime import date
from typing import Protocol

import structlog
from pydantic import ValidationError

from core.http.base import ProviderRateLimitError
from core.ingestion import (
    BronzeParseResult,
    LandingObjectRecord,
    LandingWrite,
    PipelineRunScope,
    PipelineRunTracker,
    RejectionRecord,
    RunStatus,
    RunUnitRecord,
    terminal_status,
)
from core.orchestration.events import publish_ingestion_summary
from domains.eod_price.contracts import EodPriceBackfillRequest, EodPriceDailyRequest, EodPriceRefreshResult
from domains.eod_price.models import EODBar
from domains.eod_price.parsers import infer_bulk_bar_date
from domains.eod_price.tasks import (
    EODBackfillCoverageOutcome,
    eod_price_already_ingested,
    fetch_eod_backfill_provider_exchange_codes,
    fetch_eod_price_bulk,
    fetch_eod_provider_exchange_codes,
    fetch_instrument_eod_history,
    load_backfill_pending_instruments,
    parse_eod_price,
    parse_instrument_eod_history,
    write_backfill_eod_batch,
    write_bronze_eod_price,
    write_eod_backfill_completed_coverage,
    write_eod_backfill_coverage,
    write_eod_backfill_deferred_coverage,
    write_eod_price_to_landing,
    write_instrument_eod_history_to_landing,
)
from providers.eodhd.models import EODBulkPriceRaw, EODPriceBarRaw

log = structlog.get_logger(__name__)
_REJECTION_SAMPLE_LIMIT_PER_UNIT = 100


@dataclass(frozen=True, slots=True)
class _BackfillInstrumentBatchResult:
    """Landing and parse output for one successful backfill instrument fetch."""

    provider_instrument_code: str
    unit_id: str
    unit_key: dict[str, object]
    raw_bars: list[EODPriceBarRaw]
    landing: LandingWrite
    valid: list[BronzeParseResult[EODBar]]
    rejected_rows: list[EODPriceBarRaw]


class EodPricePreflightHook(Protocol):
    """Optional pre-ingestion hook run inside the tracked app-run lifecycle."""

    async def __call__(self, *, parent_run_id: str, summary: dict[str, object]) -> dict[str, object]:
        """Run preflight work and mutate the run summary if needed."""
        ...


class EodPricePostIngestionHook(Protocol):
    """Optional post-ingestion hook run before the tracked app run is completed."""

    async def __call__(
        self,
        *,
        summary: dict[str, object],
        upstream_status: RunStatus,
        parent_run_id: str,
        provider_exchange_codes: list[str],
        from_date: date | None,
        to_date: date | None,
        exchange_dates: dict[str, date] | None,
    ) -> RunStatus:
        """Return the final audit status after post-ingestion checks."""
        ...


async def _write_landing_and_parse_backfill_instrument(
    *,
    provider_exchange_code: str,
    provider_instrument_code: str,
    unit_id: str,
    unit_key: dict[str, object],
    raw_bars: list[EODPriceBarRaw],
    from_date: date | None,
    to_date: date,
) -> _BackfillInstrumentBatchResult:
    """Write one historical landing object, then parse its bars."""
    landing = await write_instrument_eod_history_to_landing(
        raw_bars,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        from_date=from_date,
        to_date=to_date,
    )
    valid, rejected_rows = await parse_instrument_eod_history(
        raw_bars,
        provider_exchange_code=provider_exchange_code,
        provider_instrument_code=provider_instrument_code,
        source_uri=landing.source_uri,
    )
    return _BackfillInstrumentBatchResult(
        provider_instrument_code=provider_instrument_code,
        unit_id=unit_id,
        unit_key=unit_key,
        raw_bars=raw_bars,
        landing=landing,
        valid=valid,
        rejected_rows=rejected_rows,
    )


async def run_eod_price_daily(
    request: EodPriceDailyRequest,
    *,
    post_ingestion: EodPricePostIngestionHook | None = None,
) -> EodPriceRefreshResult:
    """Ingest EOD price for all (or the given) exchange on trade_date.

    Args:
        request: Daily refresh inputs. If ``trade_date`` is omitted, the
            provider returns its latest available trading day per exchange.
            Provider exchange codes default to the dbt-built EOD price universe.
        post_ingestion: Optional orchestration hook that can run after ingestion
            units are recorded but before the tracked run is completed.
    """
    trade_date = request.trade_date
    codes = (
        await fetch_eod_provider_exchange_codes()
        if request.provider_exchange_codes is None
        else request.provider_exchange_codes
    )

    total_written = 0
    total_raw = 0
    total_valid = 0
    total_rejected = 0
    coverage_exchange_dates: dict[str, date] = {}
    latest_date_mismatches: list[dict[str, str]] = []
    summary: dict = {
        "trade_date": trade_date.isoformat() if trade_date else None,
        "exchange": {},
        "failed": [],
        "latest_date_mismatches": latest_date_mismatches,
    }

    tracker = PipelineRunTracker()
    run_id: str | None = None
    run_status: RunStatus | None = None
    with tracker.track_run(
        flow_name="eod-price-daily",
        domain="eod_price",
        run_kind="daily" if trade_date is None else "adhoc_date",
        provider="eodhd",
        parameters={
            "trade_date": trade_date.isoformat() if trade_date else None,
            "provider_exchange_codes": request.provider_exchange_codes,
        },
        target_window_start=trade_date,
        target_window_end=trade_date,
    ) as run:
        run_id = str(run.run_id)
        log.info("price.flow_start", trade_date=trade_date, exchange=len(codes), run_id=run.run_id)

        try:
            for provider_exchange_code in codes:
                if trade_date is not None and eod_price_already_ingested(provider_exchange_code, trade_date):
                    log.info(
                        "price.exchange_skipped",
                        provider_exchange_code=provider_exchange_code,
                        reason="already_ingested",
                        trade_date=trade_date,
                    )
                    summary["exchange"][provider_exchange_code] = {
                        "bar_date": trade_date.isoformat(),
                        "rows_written": 0,
                    }
                    coverage_exchange_dates[provider_exchange_code] = trade_date
                    run.record_unit(
                        unit_type="exchange_date",
                        unit_key={
                            "provider_exchange_code": provider_exchange_code,
                            "bar_date": trade_date.isoformat(),
                        },
                        status="skipped",
                        reason="already_ingested",
                        rows_raw=0,
                        rows_valid=0,
                        rows_rejected=0,
                        rows_written=0,
                    )
                    continue

                can_mark_failed = True
                try:
                    raw_rows = await fetch_eod_price_bulk(
                        provider_exchange_code=provider_exchange_code,
                        bar_date=trade_date,
                    )
                    total_raw += len(raw_rows)

                    if not raw_rows:
                        log.info(
                            "price.exchange_skipped",
                            provider_exchange_code=provider_exchange_code,
                            reason="no_data",
                            trade_date=trade_date,
                        )
                        summary["exchange"][provider_exchange_code] = {"bar_date": None, "rows_written": 0}
                        if trade_date is not None:
                            coverage_exchange_dates[provider_exchange_code] = trade_date
                        can_mark_failed = False
                        run.record_unit(
                            unit_type="exchange_date",
                            unit_key={
                                "provider_exchange_code": provider_exchange_code,
                                "bar_date": trade_date.isoformat() if trade_date else None,
                            },
                            status="skipped",
                            reason="no_data",
                            rows_raw=0,
                            rows_valid=0,
                            rows_rejected=0,
                            rows_written=0,
                        )
                        continue

                    bar_date = trade_date or infer_bulk_bar_date(raw_rows)
                    landing = await write_eod_price_to_landing(
                        raw_rows,
                        provider_exchange_code=provider_exchange_code,
                        bar_date=bar_date,
                    )
                    sources, rejected_rows = parse_eod_price(
                        raw_rows,
                        bar_date=bar_date,
                        provider_exchange_code=provider_exchange_code,
                    )
                    rejected = len(rejected_rows)
                    total_valid += len(sources)
                    total_rejected += rejected
                    bronze = write_bronze_eod_price(
                        sources,
                        provider_exchange_code=provider_exchange_code,
                        bar_date=bar_date,
                        source_uri=landing.source_uri,
                    )

                    summary["exchange"][provider_exchange_code] = {
                        "bar_date": bar_date.isoformat(),
                        "rows_written": bronze.rows_written,
                    }
                    coverage_exchange_dates[provider_exchange_code] = bar_date
                    total_written += bronze.rows_written
                    can_mark_failed = False
                    unit_id = run.record_unit_with_landing(
                        landing,
                        unit_type="exchange_date",
                        unit_key={
                            "provider_exchange_code": provider_exchange_code,
                            "bar_date": bar_date.isoformat(),
                        },
                        status="completed",
                        reason=bronze.reason,
                        rows_raw=len(raw_rows),
                        rows_valid=len(sources),
                        rows_rejected=rejected,
                        rows_written=bronze.rows_written,
                    )
                    run.record_rejections(
                        _daily_rejection_records(
                            run=run,
                            unit_id=unit_id,
                            provider_exchange_code=provider_exchange_code,
                            bar_date=bar_date,
                            source_uri=landing.source_uri,
                            rejected_rows=rejected_rows,
                        )
                    )

                except ValidationError as exc:
                    # Schema drift from provider — re-raise immediately.
                    # All exchange will likely fail the same way; no point continuing.
                    summary["failed"].append(provider_exchange_code)
                    run.record_unit(
                        unit_type="exchange_date",
                        unit_key={
                            "provider_exchange_code": provider_exchange_code,
                            "bar_date": trade_date.isoformat() if trade_date else None,
                        },
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
                    if not can_mark_failed:
                        raise
                    log.error("price.exchange_failed", provider_exchange_code=provider_exchange_code, error=str(exc))
                    summary["failed"].append(provider_exchange_code)
                    run.record_unit(
                        unit_type="exchange_date",
                        unit_key={
                            "provider_exchange_code": provider_exchange_code,
                            "bar_date": trade_date.isoformat() if trade_date else None,
                        },
                        status="failed",
                        error=exc,
                        rows_written=0,
                    )

            run_status = terminal_status(
                failed=run.tally.failed,
                rejected=total_rejected,
                skipped_all=_all_units_skipped(total=run.tally.total, skipped=run.tally.skipped),
            )
            ingestion_status = run_status
            counters = run.tally.counters(
                rows_raw=total_raw,
                rows_valid=total_valid,
                rows_rejected=total_rejected,
                rows_written=total_written,
            )
            if post_ingestion is not None and run_id is not None:
                try:
                    run_status = await post_ingestion(
                        summary=summary,
                        upstream_status=ingestion_status,
                        parent_run_id=run_id,
                        provider_exchange_codes=codes,
                        from_date=trade_date,
                        to_date=trade_date,
                        exchange_dates=coverage_exchange_dates,
                    )
                except Exception as exc:
                    summary["post_ingestion_error"] = _exception_summary(exc)
                    run.complete(status=ingestion_status, counters=counters, summary=summary)
                    await publish_ingestion_summary(
                        flow_name="eod-price-daily",
                        domain="eod_price",
                        app_run_id=run.run_id,
                        status=ingestion_status,
                        summary=summary,
                    )
                    raise
            run.complete(
                status=run_status,
                counters=counters,
                summary=summary,
            )
            await publish_ingestion_summary(
                flow_name="eod-price-daily",
                domain="eod_price",
                app_run_id=run.run_id,
                status=run_status,
                summary=summary,
            )
            log.info(
                "price.flow_done",
                trade_date=trade_date,
                total_written=total_written,
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
                await publish_ingestion_summary(
                    flow_name="eod-price-daily",
                    domain="eod_price",
                    app_run_id=run.run_id,
                    status="failed",
                    summary=summary,
                )
            raise

    if run_id is None or run_status is None:
        msg = "EOD price daily refresh did not record a run result."
        raise RuntimeError(msg)
    return EodPriceRefreshResult(run_id=run_id, status=run_status, summary=summary)


def _daily_rejection_records(
    *,
    run: PipelineRunScope,
    unit_id: str,
    provider_exchange_code: str,
    bar_date: date,
    source_uri: str,
    rejected_rows: list[EODBulkPriceRaw],
) -> list[RejectionRecord]:
    """Build capped daily parser rejection records for one exchange/date unit."""
    return [
        run.rejection_record(
            unit_id=unit_id,
            entity_key={
                "provider_exchange_code": provider_exchange_code,
                "provider_instrument_code": row.code,
                "bar_date": bar_date.isoformat(),
                "raw_date": row.date,
            },
            source_uri=source_uri,
            raw_fragment=row,
            reason="parse_rejected",
        )
        for row in rejected_rows[:_REJECTION_SAMPLE_LIMIT_PER_UNIT]
    ]


def _instrument_rejection_records(
    *,
    run: PipelineRunScope,
    unit_id: str,
    provider_exchange_code: str,
    provider_instrument_code: str,
    source_uri: str,
    rejected_rows: list[EODPriceBarRaw],
) -> list[RejectionRecord]:
    """Build capped historical parser rejection records for one instrument unit."""
    return [
        run.rejection_record(
            unit_id=unit_id,
            entity_key={
                "provider_exchange_code": provider_exchange_code,
                "provider_instrument_code": provider_instrument_code,
                "raw_date": row.date,
            },
            source_uri=source_uri,
            raw_fragment=row,
            reason="parse_rejected",
        )
        for row in rejected_rows[:_REJECTION_SAMPLE_LIMIT_PER_UNIT]
    ]


def _all_units_skipped(*, total: int, skipped: int) -> bool:
    """Return True when the requested scope produced no executable work."""
    return total == 0 or skipped == total


def _iso_date(value: date | None) -> str | None:
    """Return an ISO date string, preserving open-start backfill windows as null."""
    return value.isoformat() if value else None


def _exception_summary(exc: Exception) -> dict[str, str]:
    """Return a compact JSON-safe exception summary for run metadata."""
    return {"type": type(exc).__name__, "message": str(exc)[-2000:]}


async def run_eod_price_backfill(
    request: EodPriceBackfillRequest,
    *,
    preflight: EodPricePreflightHook | None = None,
    post_ingestion: EodPricePostIngestionHook | None = None,
) -> EodPriceRefreshResult:
    """Ingest full OHLCV history for every latest EODHD provider instrument.

    Processes each exchange sequentially; within an exchange, fetches
    ``batch_size`` instruments concurrently via ``asyncio.gather``. After each
    batch all bars are committed to bronze before the next batch starts,
    giving natural checkpoints for resume on failure.

    ``from_date = None`` omits the provider ``from`` parameter and requests all
    available EODHD history through ``to_date``. Explicit windows use the
    dbt-built instrument-day coverage view, so first/last price spans with
    middle gaps stay pending. An instrument is skipped when terminal coverage
    has the exact same backfill unit key:
    ``provider_exchange_code``, ``provider_instrument_code``, ``from_date``, and ``to_date``.
    Re-runs are safe after rebuilding those Silver views. To retry an instrument,
    delete its price and/or coverage rows for that exchange partition first.
    Parser-rejected payloads are not marked ``no_data``; they remain retryable.

    Provider-validated backfill payloads are written to S3 landing before
    parsing so historical ingestion follows the same replay contract as daily
    and reference flows.

    Args:
        request: Backfill inputs, including date window, provider exchange
            scope, batch size, and optional provider-call cap.
        preflight: Optional orchestration hook that can run before backfill
            selection inside the tracked run.
        post_ingestion: Optional orchestration hook that can run after ingestion
            units are recorded but before the tracked run is completed.
    """
    from_date = request.from_date
    to_date = request.to_date or date.today()
    codes = (
        await fetch_eod_backfill_provider_exchange_codes()
        if request.provider_exchange_codes is None
        else request.provider_exchange_codes
    )
    batch_size = request.batch_size
    provider_call_limit = None if request.max_provider_calls is None else max(0, int(request.max_provider_calls))
    provider_calls_submitted = 0
    provider_calls_deferred = 0

    total_written = 0
    total_raw = 0
    total_valid = 0
    total_rejected = 0
    summary: dict = {
        "from_date": _iso_date(from_date),
        "to_date": to_date.isoformat(),
        "exchange": {},
        "failed_instruments": [],
        "provider_quota_exhausted": False,
        "provider_calls": {
            "max": provider_call_limit,
            "submitted": 0,
            "deferred": 0,
        },
    }
    tracker = PipelineRunTracker()
    run_id: str | None = None
    run_status: RunStatus | None = None
    with tracker.track_run(
        flow_name="eod-price-backfill",
        domain="eod_price",
        run_kind="historical_backfill",
        provider="eodhd",
        parameters={
            "from_date": _iso_date(from_date),
            "to_date": to_date.isoformat(),
            "provider_exchange_codes": request.provider_exchange_codes,
            "batch_size": request.batch_size,
            "max_provider_calls": request.max_provider_calls,
        },
        target_window_start=from_date,
        target_window_end=to_date,
    ) as run:
        run_id = str(run.run_id)
        log.info(
            "backfill.flow_start",
            from_date=from_date,
            to_date=to_date,
            exchange=len(codes),
            batch_size=batch_size,
            run_id=run.run_id,
        )

        try:
            if preflight is not None and run_id is not None:
                await preflight(parent_run_id=run_id, summary=summary)

            stop_after_exchange = False
            for provider_exchange_code in codes:
                pending_all = load_backfill_pending_instruments(provider_exchange_code, from_date, to_date)

                if not pending_all:
                    log.info("backfill.exchange_skip", provider_exchange_code=provider_exchange_code, reason="all_done")
                    summary["exchange"][provider_exchange_code] = {"instruments": 0, "rows": 0}
                    run.record_unit(
                        unit_type="exchange_backfill",
                        unit_key={
                            "provider_exchange_code": provider_exchange_code,
                            "from_date": _iso_date(from_date),
                            "to_date": to_date.isoformat(),
                        },
                        status="skipped",
                        reason="all_done",
                        rows_written=0,
                    )
                    continue

                pending = pending_all
                exchange_deferred = 0
                if provider_call_limit is not None:
                    remaining_call_budget = provider_call_limit - provider_calls_submitted
                    if remaining_call_budget <= 0:
                        provider_calls_deferred += len(pending_all)
                        write_eod_backfill_deferred_coverage(
                            run_id=str(run.run_id),
                            provider_exchange_code=provider_exchange_code,
                            provider_instrument_codes=pending_all,
                            from_date=from_date,
                            to_date=to_date,
                            reason="provider_call_budget_exhausted",
                        )
                        summary["provider_quota_exhausted"] = True
                        summary["provider_calls"]["deferred"] = provider_calls_deferred
                        summary["exchange"][provider_exchange_code] = {
                            "instruments_pending": len(pending_all),
                            "instruments_deferred": len(pending_all),
                            "rows_written": 0,
                        }
                        run.record_unit(
                            unit_type="exchange_backfill",
                            unit_key={
                                "provider_exchange_code": provider_exchange_code,
                                "from_date": _iso_date(from_date),
                                "to_date": to_date.isoformat(),
                            },
                            status="skipped",
                            reason="provider_call_budget_exhausted",
                            rows_written=0,
                        )
                        break
                    pending = pending_all[:remaining_call_budget]
                    exchange_deferred = len(pending_all) - len(pending)
                    deferred_instruments = pending_all[len(pending) :]
                    if exchange_deferred:
                        write_eod_backfill_deferred_coverage(
                            run_id=str(run.run_id),
                            provider_exchange_code=provider_exchange_code,
                            provider_instrument_codes=deferred_instruments,
                            from_date=from_date,
                            to_date=to_date,
                            reason="provider_call_budget_exhausted",
                        )
                        summary["provider_quota_exhausted"] = True
                        stop_after_exchange = True

                log.info("backfill.exchange_start", provider_exchange_code=provider_exchange_code, pending=len(pending))
                exchange_written = 0
                exchange_failed: list[str] = []
                exchange_raw = 0
                exchange_valid = 0
                exchange_rejected = 0
                exchange_rate_limited = False

                for i in range(0, len(pending), batch_size):
                    batch_instruments = pending[i : i + batch_size]
                    provider_calls_submitted += len(batch_instruments)
                    summary["provider_calls"]["submitted"] = provider_calls_submitted
                    raw_results = await asyncio.gather(
                        *[
                            fetch_instrument_eod_history(provider_exchange_code, code, from_date, to_date)
                            for code in batch_instruments
                        ],
                        return_exceptions=True,
                    )

                    batch_sources: list[BronzeParseResult[EODBar]] = []
                    unit_records: list[RunUnitRecord] = []
                    landing_records: list[LandingObjectRecord] = []
                    rejection_records: list[RejectionRecord] = []
                    completed_coverage_outcomes: list[EODBackfillCoverageOutcome] = []
                    landing_inputs: list[tuple[str, str, dict[str, object], list[EODPriceBarRaw]]] = []

                    for provider_instrument_code, result in zip(batch_instruments, raw_results, strict=True):
                        unit_id = str(uuid.uuid4())
                        unit_key: dict[str, object] = {
                            "provider_exchange_code": provider_exchange_code,
                            "provider_instrument_code": provider_instrument_code,
                            "from_date": _iso_date(from_date),
                            "to_date": to_date.isoformat(),
                        }
                        if isinstance(result, ValidationError):
                            summary["failed_instruments"].append(provider_instrument_code)
                            run.record_unit(
                                unit_id=unit_id,
                                unit_type="instrument_backfill",
                                unit_key=unit_key,
                                status="failed",
                                error=result,
                                rows_written=0,
                            )
                            raise result  # schema drift — abort everything
                        if isinstance(result, ProviderRateLimitError):
                            log.warning(
                                "backfill.provider_quota_exhausted",
                                provider_exchange_code=provider_exchange_code,
                                provider_instrument_code=provider_instrument_code,
                                retry_after=result.retry_after,
                            )
                            summary["provider_quota_exhausted"] = True
                            exchange_rate_limited = True
                            exchange_failed.append(provider_instrument_code)
                            unit_records.append(
                                run.unit_record(
                                    unit_id=unit_id,
                                    unit_type="instrument_backfill",
                                    unit_key=unit_key,
                                    status="failed",
                                    reason="provider_rate_limited",
                                    error=result,
                                    rows_written=0,
                                )
                            )
                            continue
                        if isinstance(result, BaseException):
                            log.error(
                                "backfill.instrument_failed",
                                provider_exchange_code=provider_exchange_code,
                                provider_instrument_code=provider_instrument_code,
                                error=str(result),
                            )
                            exchange_failed.append(provider_instrument_code)
                            unit_records.append(
                                run.unit_record(
                                    unit_id=unit_id,
                                    unit_type="instrument_backfill",
                                    unit_key=unit_key,
                                    status="failed",
                                    error=result,
                                    rows_written=0,
                                )
                            )
                            continue

                        landing_inputs.append((provider_instrument_code, unit_id, unit_key, result))

                    parsed_results = await asyncio.gather(
                        *[
                            _write_landing_and_parse_backfill_instrument(
                                provider_exchange_code=provider_exchange_code,
                                provider_instrument_code=provider_instrument_code,
                                unit_id=unit_id,
                                unit_key=unit_key,
                                raw_bars=raw_bars,
                                from_date=from_date,
                                to_date=to_date,
                            )
                            for provider_instrument_code, unit_id, unit_key, raw_bars in landing_inputs
                        ],
                        return_exceptions=True,
                    )
                    instrument_results: list[_BackfillInstrumentBatchResult] = []
                    instrument_errors: list[BaseException] = []
                    for parsed in parsed_results:
                        if isinstance(parsed, BaseException):
                            instrument_errors.append(parsed)
                        else:
                            instrument_results.append(parsed)
                    if instrument_errors:
                        for (
                            provider_instrument_code,
                            _,
                            _,
                            _,
                        ), parsed in zip(landing_inputs, parsed_results, strict=True):
                            if isinstance(parsed, BaseException):
                                log.error(
                                    "backfill.landing_or_parse_failed",
                                    provider_exchange_code=provider_exchange_code,
                                    provider_instrument_code=provider_instrument_code,
                                    error=str(parsed),
                                )
                        raise instrument_errors[0]

                    for instrument_result in instrument_results:
                        provider_instrument_code = instrument_result.provider_instrument_code
                        unit_id = instrument_result.unit_id
                        unit_key = instrument_result.unit_key
                        result = instrument_result.raw_bars
                        landing = instrument_result.landing
                        valid = instrument_result.valid
                        rejected_rows = instrument_result.rejected_rows
                        rejected = len(rejected_rows)
                        total_raw += len(result)
                        total_valid += len(valid)
                        total_rejected += rejected
                        exchange_raw += len(result)
                        exchange_valid += len(valid)
                        exchange_rejected += rejected
                        if valid:
                            batch_sources.extend(valid)
                            if not rejected_rows:
                                completed_coverage_outcomes.append(
                                    {
                                        "provider_instrument_code": provider_instrument_code,
                                        "rows_raw": len(result),
                                        "rows_valid": len(valid),
                                        "rows_rejected": rejected,
                                        "source_uri": landing.source_uri,
                                    }
                                )
                        elif not result:
                            write_eod_backfill_coverage(
                                run_id=str(run.run_id),
                                provider_exchange_code=provider_exchange_code,
                                provider_instrument_code=provider_instrument_code,
                                from_date=from_date,
                                to_date=to_date,
                                rows_raw=len(result),
                                rows_valid=len(valid),
                                rows_rejected=rejected,
                                source_uri=landing.source_uri,
                            )
                        unit_reason = None
                        if not valid:
                            unit_reason = "no_valid_rows" if not result else "all_rows_rejected"
                        unit_records.append(
                            run.unit_record(
                                unit_id=unit_id,
                                unit_type="instrument_backfill",
                                unit_key=unit_key,
                                status="completed",
                                reason=unit_reason,
                                source_uri=landing.source_uri,
                                rows_raw=len(result),
                                rows_valid=len(valid),
                                rows_rejected=rejected,
                            )
                        )
                        landing_records.append(
                            run.landing_object_record(
                                landing,
                                unit_id=unit_id,
                            )
                        )
                        rejection_records.extend(
                            _instrument_rejection_records(
                                run=run,
                                unit_id=unit_id,
                                provider_exchange_code=provider_exchange_code,
                                provider_instrument_code=provider_instrument_code,
                                source_uri=landing.source_uri,
                                rejected_rows=rejected_rows,
                            )
                        )

                    batch_written = 0
                    if batch_sources:
                        bronze_batch = write_backfill_eod_batch(
                            batch_sources,
                            provider_exchange_code=provider_exchange_code,
                        )
                        batch_written = bronze_batch.rows_written
                        exchange_written += batch_written
                    if completed_coverage_outcomes:
                        write_eod_backfill_completed_coverage(
                            run_id=str(run.run_id),
                            provider_exchange_code=provider_exchange_code,
                            from_date=from_date,
                            to_date=to_date,
                            outcomes=completed_coverage_outcomes,
                        )

                    run.record_units(unit_records)
                    run.record_landing_objects(landing_records)
                    run.record_rejections(rejection_records)

                    log.info(
                        "backfill.batch_done",
                        provider_exchange_code=provider_exchange_code,
                        batch=i // batch_size + 1,
                        of=(len(pending) + batch_size - 1) // batch_size,
                        rows_written=batch_written,
                        units=len(unit_records),
                    )
                    if exchange_rate_limited:
                        deferred_instruments = pending[min(i + batch_size, len(pending)) :]
                        exchange_deferred += len(deferred_instruments)
                        if deferred_instruments:
                            write_eod_backfill_deferred_coverage(
                                run_id=str(run.run_id),
                                provider_exchange_code=provider_exchange_code,
                                provider_instrument_codes=deferred_instruments,
                                from_date=from_date,
                                to_date=to_date,
                                reason="provider_rate_limited",
                            )
                        stop_after_exchange = True
                        break

                summary["exchange"][provider_exchange_code] = {
                    "instruments_pending": len(pending_all),
                    "instruments_failed": len(exchange_failed),
                    "instruments_deferred": exchange_deferred,
                    "rows_written": exchange_written,
                }
                if exchange_deferred:
                    provider_calls_deferred += exchange_deferred
                    summary["provider_calls"]["deferred"] = provider_calls_deferred
                exchange_status = "completed" if not exchange_failed else "failed"
                run.record_unit(
                    unit_type="exchange_backfill",
                    unit_key={
                        "provider_exchange_code": provider_exchange_code,
                        "from_date": _iso_date(from_date),
                        "to_date": to_date.isoformat(),
                    },
                    status=exchange_status,
                    reason="instrument_failures" if exchange_failed else None,
                    rows_raw=exchange_raw,
                    rows_valid=exchange_valid,
                    rows_rejected=exchange_rejected,
                    rows_written=exchange_written,
                )
                summary["failed_instruments"].extend(exchange_failed)
                total_written += exchange_written
                if stop_after_exchange:
                    break

            run_status = terminal_status(
                failed=run.tally.failed,
                rejected=total_rejected,
                skipped_all=_all_units_skipped(total=run.tally.total, skipped=run.tally.skipped),
            )
            ingestion_status = run_status
            counters = run.tally.counters(
                rows_raw=total_raw,
                rows_valid=total_valid,
                rows_rejected=total_rejected,
                rows_written=total_written,
            )
            if post_ingestion is not None and run_id is not None:
                try:
                    run_status = await post_ingestion(
                        summary=summary,
                        upstream_status=ingestion_status,
                        parent_run_id=run_id,
                        provider_exchange_codes=codes,
                        from_date=from_date,
                        to_date=to_date,
                        exchange_dates=None,
                    )
                except Exception as exc:
                    summary["post_ingestion_error"] = _exception_summary(exc)
                    run.complete(status=ingestion_status, counters=counters, summary=summary)
                    await publish_ingestion_summary(
                        flow_name="eod-price-backfill",
                        domain="eod_price",
                        app_run_id=run.run_id,
                        status=ingestion_status,
                        summary=summary,
                    )
                    raise
            run.complete(
                status=run_status,
                counters=counters,
                summary=summary,
            )
            await publish_ingestion_summary(
                flow_name="eod-price-backfill",
                domain="eod_price",
                app_run_id=run.run_id,
                status=run_status,
                summary=summary,
            )
            log.info(
                "backfill.flow_done",
                total_written=total_written,
                failed_instruments=len(summary["failed_instruments"]),
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
                await publish_ingestion_summary(
                    flow_name="eod-price-backfill",
                    domain="eod_price",
                    app_run_id=run.run_id,
                    status="failed",
                    summary=summary,
                )
            raise

    if run_id is None or run_status is None:
        msg = "EOD price backfill did not record a run result."
        raise RuntimeError(msg)
    return EodPriceRefreshResult(run_id=run_id, status=run_status, summary=summary)


__all__ = [
    "EodPricePostIngestionHook",
    "EodPricePreflightHook",
    "run_eod_price_backfill",
    "run_eod_price_daily",
]
