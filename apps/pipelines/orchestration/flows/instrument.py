"""Instrument ingestion Prefect flow."""

from datetime import date

from prefect import flow

from config.domains import Domain
from domains.instrument.contracts import InstrumentRefreshRequest
from domains.instrument.service import run_instrument_refresh
from orchestration.domain_dbt import DOMAIN_DBT_REGISTRY
from orchestration.post_ingestion import run_dbt_build_after_ingestion


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
        run_dbt_build=run_dbt_build,
    )
    result = await run_instrument_refresh(request)
    summary = result.summary
    if run_dbt_build:
        build = DOMAIN_DBT_REGISTRY[Domain.INSTRUMENT].post_ingestion_build
        if build is None:
            msg = "Instrument domain has no configured post-ingestion dbt build."
            raise RuntimeError(msg)
        summary["dbt_build"] = await run_dbt_build_after_ingestion(
            enabled=run_dbt_build,
            build=build,
            upstream_status=result.status,
            parent_run_id=result.run_id,
        )
    return summary


__all__ = ["instrument_flow"]
