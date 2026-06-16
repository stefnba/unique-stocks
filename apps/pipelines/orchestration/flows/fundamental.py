"""Fundamentals Prefect flow."""

from datetime import date

from prefect import flow

from config.domains import Domain
from domains.fundamental.contracts import FundamentalRefreshRequest
from domains.fundamental.service import run_fundamental_refresh
from orchestration.post_ingestion import post_ingestion_build_for_domain, run_dbt_build_after_ingestion


@flow(
    name="fundamental-quarterly",
    description=(
        "Ingest EODHD fundamental JSON per provider_instrument_code. Writes S3 landing, document metadata, "
        "and curated bronze.fundamental_* slices for active marts."
    ),
)
async def fundamental_flow(
    provider_instruments: list[dict[str, str]] | None = None,
    snapshot_date: date | None = None,
    ingestion_batch_date: date | None = None,
    continue_ingestion_batch: bool = False,
    provider_exchange_codes: list[str] | None = None,
    limit: int | None = None,
    skip_existing: bool = True,
    refresh_existing: bool = False,
    replay_landing: bool = False,
    landing_source_uris_by_instrument: dict[str, str] | None = None,
    batch_size: int = 1,
    provider_batch_delay_seconds: float = 0.0,
    provider_credits_per_call: int = 10,
    max_provider_credits: int | None = None,
    run_dbt_build: bool = False,
) -> dict[str, object]:
    """Ingest fundamentals for explicit provider instruments or latest provider instruments."""
    result = await run_fundamental_refresh(
        FundamentalRefreshRequest(
            provider_instruments=provider_instruments,
            snapshot_date=snapshot_date,
            ingestion_batch_date=ingestion_batch_date,
            continue_ingestion_batch=continue_ingestion_batch,
            provider_exchange_codes=provider_exchange_codes,
            limit=limit,
            skip_existing=skip_existing,
            refresh_existing=refresh_existing,
            replay_landing=replay_landing,
            landing_source_uris_by_instrument=landing_source_uris_by_instrument,
            batch_size=batch_size,
            provider_batch_delay_seconds=provider_batch_delay_seconds,
            provider_credits_per_call=provider_credits_per_call,
            max_provider_credits=max_provider_credits,
        )
    )
    summary = result.summary
    if run_dbt_build:
        summary["dbt_build"] = await run_dbt_build_after_ingestion(
            enabled=run_dbt_build,
            build=post_ingestion_build_for_domain(Domain.FUNDAMENTAL),
            upstream_status=result.status,
            parent_run_id=result.run_id,
        )
    return summary


__all__ = ["fundamental_flow"]
