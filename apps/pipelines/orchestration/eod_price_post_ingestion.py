"""EOD price post-ingestion dbt and coverage-gate orchestration."""

import inspect
from datetime import date

import structlog
from prefect.artifacts import create_table_artifact
from prefect.context import get_run_context
from prefect.transactions import transaction

from config.domains import Domain
from control_plane.prefect.dbt_builds import DbtBuildDeployment
from core.ingestion.run_tracking import RunStatus
from core.lake import reset_lake_client
from core.orchestration.events import emit_coverage_gate_failure
from domains.eod_price.tasks import (
    EODPriceCoverageGap,
    load_eod_latest_expected_exchange_dates,
    load_eod_price_coverage_gaps,
    load_missing_eod_backfill_selection_views,
)
from orchestration.post_ingestion import (
    post_ingestion_build_for_domain,
    run_dbt_build_after_ingestion,
    run_dbt_build_deployment,
)

log = structlog.get_logger(__name__)


async def run_price_post_ingestion_checks(
    *,
    summary: dict[str, object],
    upstream_status: RunStatus,
    parent_run_id: str,
    provider_exchange_codes: list[str],
    from_date: date | None,
    to_date: date | None,
    exchange_dates: dict[str, date] | None,
) -> RunStatus:
    """Run price dbt and downgrade the audit status when the coverage gate finds gaps."""
    gate_exchange_dates = dict(exchange_dates) if exchange_dates is not None else None
    gated_status = upstream_status
    if gate_exchange_dates is not None and from_date is None and to_date is None:
        gated_status = _apply_provider_latest_expectations(
            summary=summary,
            upstream_status=upstream_status,
            provider_exchange_codes=provider_exchange_codes,
            exchange_dates=gate_exchange_dates,
        )

    build = post_ingestion_build_for_domain(Domain.EOD_PRICE)
    reset_lake_client()
    force_skipped_gate = gated_status == "skipped" and bool(gate_exchange_dates)
    try:
        if force_skipped_gate:
            summary["dbt_build"] = await run_dbt_build_deployment(
                build=build,
                parent_run_id=parent_run_id,
                idempotency_key=f"{parent_run_id}:{build}",
                tags=["post-ingestion-dbt", build],
            )
        else:
            summary["dbt_build"] = await run_dbt_build_after_ingestion(
                enabled=True,
                build=build,
                upstream_status=gated_status,
                parent_run_id=parent_run_id,
            )
    finally:
        reset_lake_client()

    dbt_build = summary["dbt_build"]
    if not isinstance(dbt_build, dict) or not dbt_build.get("triggered"):
        summary["coverage_gate"] = {
            "status": "skipped",
            "reason": "dbt_build_not_triggered",
            "upstream_status": gated_status,
        }
        return gated_status

    gaps = load_eod_price_coverage_gaps(
        provider_exchange_codes=provider_exchange_codes,
        from_date=from_date,
        to_date=to_date,
        exchange_dates=gate_exchange_dates,
    )
    summary["coverage_gate"] = _coverage_gate_summary(gaps)
    await _emit_coverage_gate_artifact(gaps=gaps, parent_run_id=parent_run_id)
    if gaps:
        log.warning(
            "price.coverage_gate_failed",
            gaps=len(gaps),
            provider_exchange_codes=provider_exchange_codes,
            from_date=from_date,
            to_date=to_date,
        )
        emit_coverage_gate_failure(
            app_run_id=parent_run_id,
            gaps_count=len(gaps),
            provider_exchange_codes=provider_exchange_codes,
            from_date=_iso_date(from_date) or "open",
            to_date=to_date.isoformat() if to_date else "latest",
        )
        return "partial"
    log.info("price.coverage_gate_passed", provider_exchange_codes=provider_exchange_codes)
    return gated_status


async def build_price_selection_views_if_missing(
    *,
    parent_run_id: str,
    summary: dict[str, object],
) -> dict[str, object]:
    """Build ingestion-control selector views when historical backfill needs them."""
    build_name: DbtBuildDeployment = "ingestion-control-build"
    missing_before = load_missing_eod_backfill_selection_views()
    if not missing_before:
        result: dict[str, object] = {
            "enabled": True,
            "triggered": False,
            "build": build_name,
            "reason": "selection_views_present",
            "missing": [],
        }
        summary["preflight_dbt_build"] = result
        return result

    log.info("backfill.selection_views_missing", missing=missing_before)
    preflight_summary: dict[str, object] = {
        "enabled": True,
        "triggered": True,
        "build": build_name,
        "missing_before": missing_before,
    }
    summary["preflight_dbt_build"] = preflight_summary
    reset_lake_client()
    try:
        with transaction(key=f"eod-price-selection-views:{','.join(sorted(missing_before))}"):
            result = await run_dbt_build_deployment(
                build=build_name,
                parent_run_id=parent_run_id,
                idempotency_key=f"{parent_run_id}:preflight:{build_name}",
                tags=["preflight-dbt", build_name],
            )
    except Exception as exc:
        preflight_summary["status"] = "failed"
        preflight_summary["error"] = _exception_summary(exc)
        raise
    finally:
        reset_lake_client()

    missing_after = load_missing_eod_backfill_selection_views()
    if missing_after:
        result["reason"] = "missing_selection_views"
        result["missing_before"] = missing_before
        result["missing_after"] = missing_after
        result["status"] = "failed"
        summary["preflight_dbt_build"] = result
        missing = ", ".join(f"silver.{table}" for table in missing_after)
        raise RuntimeError(
            f"dbt-build/{build_name} completed but required backfill selector views are missing: {missing}"
        )
    result["reason"] = "missing_selection_views"
    result["missing_before"] = missing_before
    result["missing_after"] = []
    summary["preflight_dbt_build"] = result
    return result


def _apply_provider_latest_expectations(
    *,
    summary: dict[str, object],
    upstream_status: RunStatus,
    provider_exchange_codes: list[str],
    exchange_dates: dict[str, date],
) -> RunStatus:
    """Apply expected latest trading dates before deciding whether dbt may run."""
    expected_dates = load_eod_latest_expected_exchange_dates(provider_exchange_codes, date.today())
    mismatches: list[dict[str, str]] = []
    exchange_summary = summary.get("exchange")
    for provider_exchange_code, expected_bar_date in expected_dates.items():
        provider_bar_date = exchange_dates.get(provider_exchange_code)
        if provider_bar_date is not None and provider_bar_date != expected_bar_date:
            mismatch = {
                "provider_exchange_code": provider_exchange_code,
                "provider_bar_date": provider_bar_date.isoformat(),
                "expected_bar_date": expected_bar_date.isoformat(),
            }
            mismatches.append(mismatch)
            log.warning("price.latest_date_mismatch", **mismatch)
        if provider_bar_date is None or provider_bar_date != expected_bar_date:
            exchange_dates[provider_exchange_code] = expected_bar_date
        if isinstance(exchange_summary, dict):
            exchange_row = exchange_summary.get(provider_exchange_code)
            if isinstance(exchange_row, dict):
                exchange_row["expected_bar_date"] = expected_bar_date.isoformat()

    summary["latest_date_mismatches"] = mismatches
    if mismatches and upstream_status == "completed":
        return "partial"
    return upstream_status


def _coverage_gate_summary(gaps: list[EODPriceCoverageGap]) -> dict[str, object]:
    """Return compact run-summary metadata for exchange/day coverage gaps."""
    by_status: dict[str, int] = {}
    by_tier: dict[str, int] = {}
    by_daily_mode: dict[str, int] = {}
    for gap in gaps:
        status = str(gap["exchange_day_status"])
        by_status[status] = by_status.get(status, 0) + 1
        tier = str(gap.get("universe_tier", "unknown"))
        by_tier[tier] = by_tier.get(tier, 0) + 1
        daily_mode = str(gap.get("daily_coverage_mode", "unknown"))
        by_daily_mode[daily_mode] = by_daily_mode.get(daily_mode, 0) + 1
    return {
        "status": "failed" if gaps else "passed",
        "scope": "blocking_latest_daily_coverage",
        "blocking_flag": "is_blocking_coverage_gap",
        "gaps": len(gaps),
        "by_status": by_status,
        "by_tier": by_tier,
        "by_daily_mode": by_daily_mode,
        "sample": [_coverage_gap_summary_row(gap) for gap in gaps[:20]],
    }


def _coverage_gap_summary_row(gap: EODPriceCoverageGap) -> dict[str, object]:
    """Return a JSON-safe compact representation of one coverage gap."""
    bar_date = gap["bar_date"]
    latest_expected_bar_date = gap.get("latest_expected_bar_date")
    return {
        "provider_exchange_code": gap["provider_exchange_code"],
        "bar_date": bar_date.isoformat() if isinstance(bar_date, date) else str(bar_date),
        "universe_tier": gap.get("universe_tier", "unknown"),
        "daily_coverage_mode": gap.get("daily_coverage_mode", "unknown"),
        "latest_expected_bar_date": (
            latest_expected_bar_date.isoformat()
            if isinstance(latest_expected_bar_date, date)
            else latest_expected_bar_date
        ),
        "is_blocking_coverage_gap": gap.get("is_blocking_coverage_gap", True),
        "exchange_day_status": gap["exchange_day_status"],
        "expected_instruments": gap["expected_instruments"],
        "priced_instruments": gap["priced_instruments"],
        "missing_price_instruments": gap["missing_price_instruments"],
        "unknown_calendar_instruments": gap["unknown_calendar_instruments"],
        "unknown_calendar_coverage_instruments": gap["unknown_calendar_coverage_instruments"],
        "unknown_instrument_lifecycle_instruments": gap["unknown_instrument_lifecycle_instruments"],
    }


async def _emit_coverage_gate_artifact(*, gaps: list[EODPriceCoverageGap], parent_run_id: str) -> None:
    """Publish a table artifact with a bounded sample of coverage gaps."""
    if not gaps:
        return
    try:
        get_run_context()
    except RuntimeError:
        log.debug(
            "price.coverage_gate_artifact_skipped",
            parent_run_id=parent_run_id,
            reason="missing_run_context",
        )
        return
    rows = [_coverage_gap_summary_row(gap) for gap in gaps[:100]]
    try:
        artifact_id = create_table_artifact(
            key=f"eod-price-coverage-{parent_run_id}",
            table=rows,
            description=f"EOD price coverage gate found {len(gaps)} gap(s).",
        )
        if inspect.isawaitable(artifact_id):
            await artifact_id
    except Exception:
        log.warning("price.coverage_gate_artifact_failed", parent_run_id=parent_run_id, exc_info=True)


def _exception_summary(exc: Exception) -> dict[str, str]:
    """Return a compact JSON-safe exception summary for run metadata."""
    return {"type": type(exc).__name__, "message": str(exc)[-2000:]}


def _iso_date(value: date | None) -> str | None:
    """Return an ISO date string, preserving open-start backfill windows as null."""
    return value.isoformat() if value else None


__all__ = ["build_price_selection_views_if_missing", "run_price_post_ingestion_checks"]
