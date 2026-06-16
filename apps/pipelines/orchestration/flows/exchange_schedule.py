"""Exchange schedule Prefect flow."""

from datetime import date

from prefect import flow

from config.domains import Domain
from domains.exchange_schedule.contracts import ExchangeScheduleRefreshRequest
from domains.exchange_schedule.service import run_exchange_schedule_refresh
from orchestration.post_ingestion import post_ingestion_build_for_domain, run_dbt_build_after_ingestion


@flow(
    name="exchange-schedule-refresh",
    description=(
        "Ingest trading hours and holidays for operational provider schedule API codes. "
        "Writes bronze.exchange_schedule and bronze.exchange_holiday. Skips exchanges already ingested for "
        "snapshot_date. Provider fetches run in bounded batches; landing and Bronze writes stay sequential."
    ),
)
async def exchange_schedule_flow(
    snapshot_date: date | None = None,
    provider_schedule_exchange_codes: list[str] | None = None,
    batch_size: int = 10,
    provider_batch_delay_seconds: float = 0.0,
    run_dbt_build: bool = False,
) -> dict[str, object]:
    """Ingest exchange schedule and holiday for the operational provider schedule universe."""
    result = await run_exchange_schedule_refresh(
        ExchangeScheduleRefreshRequest(
            snapshot_date=snapshot_date,
            provider_schedule_exchange_codes=provider_schedule_exchange_codes,
            batch_size=batch_size,
            provider_batch_delay_seconds=provider_batch_delay_seconds,
        )
    )
    summary = result.summary
    if run_dbt_build:
        summary["dbt_build"] = await run_dbt_build_after_ingestion(
            enabled=run_dbt_build,
            build=post_ingestion_build_for_domain(Domain.EXCHANGE_SCHEDULE),
            upstream_status=result.status,
            parent_run_id=result.run_id,
        )
    return summary


__all__ = ["exchange_schedule_flow"]
