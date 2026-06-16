"""Instrument ingestion Prefect flow."""

from datetime import date

from prefect import flow

from config.domains import Domain
from domains.instrument.contracts import InstrumentRefreshRequest
from domains.instrument.service import run_instrument_refresh
from orchestration.post_ingestion import post_ingestion_build_for_domain, run_dbt_build_after_ingestion


@flow(
    name="instrument-refresh",
    description=(
        "Ingest active instruments (equities, ETFs, funds, FX, crypto, etc.) per provider exchange code. "
        "Writes S3 landing + bronze.instrument. Skips exchanges already ingested for snapshot_date."
    ),
)
async def instrument_flow(
    snapshot_date: date | None = None,
    provider_exchange_codes: list[str] | None = None,
    run_dbt_build: bool = False,
) -> dict[str, object]:
    """Ingest active instrument per exchange."""
    request = InstrumentRefreshRequest(
        snapshot_date=snapshot_date,
        provider_exchange_codes=provider_exchange_codes,
    )
    result = await run_instrument_refresh(request)
    summary = result.summary
    if run_dbt_build:
        summary["dbt_build"] = await run_dbt_build_after_ingestion(
            enabled=run_dbt_build,
            build=post_ingestion_build_for_domain(Domain.INSTRUMENT),
            upstream_status=result.status,
            parent_run_id=result.run_id,
        )
    return summary


__all__ = ["instrument_flow"]
