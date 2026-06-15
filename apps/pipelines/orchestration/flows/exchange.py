"""Exchange reference Prefect flows."""

from datetime import date

from prefect import flow

from domains.exchange.contracts import ExchangeCatalogRefreshRequest, ExchangeMicRegistryRefreshRequest
from domains.exchange.service import run_exchange_catalog_refresh, run_exchange_mic_registry_refresh
from orchestration.flows.exchange_schedule import exchange_schedule_flow
from orchestration.post_ingestion import run_dbt_build_deployment


@flow(
    name="exchange-catalog-refresh",
    description=(
        "Fetch the provider exchange catalog (supported MICs/codes) and write "
        "S3 landing + bronze.exchange_catalog for today's snapshot."
    ),
)
async def exchange_catalog_flow() -> int:
    """Fetch the provider exchange catalog and write landing + Bronze snapshots."""
    result = await run_exchange_catalog_refresh(ExchangeCatalogRefreshRequest())
    return result.rows_written


@flow(
    name="exchange-mic-registry-refresh",
    description=("Download the ISO 10383 MIC registry CSV and write S3 landing + bronze.exchange_mic_registry."),
)
async def exchange_mic_registry_flow(snapshot_date: date | None = None) -> dict[str, object]:
    """Fetch the ISO MIC registry CSV and write landing + Bronze snapshots."""
    result = await run_exchange_mic_registry_refresh(ExchangeMicRegistryRefreshRequest(snapshot_date=snapshot_date))
    return result.summary


@flow(
    name="exchange-reference-refresh",
    description=(
        "Refresh exchange reference inputs in order: provider catalog, ISO MIC registry, "
        "exchange dbt contract, scoped provider schedules, and exchange calendars."
    ),
)
async def exchange_reference_refresh_flow(
    snapshot_date: date | None = None,
    schedule_snapshot_date: date | None = None,
    schedule_batch_size: int = 10,
    provider_batch_delay_seconds: float = 0.0,
    run_dbt_build: bool = True,
) -> dict[str, object]:
    """Run the full exchange reference refresh chain."""
    snapshot_date = snapshot_date or date.today()
    schedule_snapshot_date = schedule_snapshot_date or snapshot_date
    summary: dict[str, object] = {
        "snapshot_date": snapshot_date.isoformat(),
        "schedule_snapshot_date": schedule_snapshot_date.isoformat(),
    }

    summary["exchange_catalog_rows_written"] = await exchange_catalog_flow()
    summary["exchange_mic_registry"] = await exchange_mic_registry_flow(snapshot_date=snapshot_date)

    if run_dbt_build:
        summary["exchange_build_before_schedule"] = await run_dbt_build_deployment(
            build="exchange-build",
            parent_run_id=None,
            tags=["exchange-reference-refresh", "exchange-build", "pre-schedule"],
        )
    else:
        summary["exchange_build_before_schedule"] = {"enabled": False, "triggered": False, "build": "exchange-build"}

    summary["exchange_schedule"] = await exchange_schedule_flow(
        snapshot_date=schedule_snapshot_date,
        batch_size=schedule_batch_size,
        provider_batch_delay_seconds=provider_batch_delay_seconds,
        run_dbt_build=run_dbt_build,
    )
    return summary


__all__ = ["exchange_catalog_flow", "exchange_mic_registry_flow", "exchange_reference_refresh_flow"]
