"""Request and result contracts for EOD price ingestion."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import RunStatus


@dataclass(frozen=True, slots=True)
class EodPriceDailyRequest:
    """Inputs for one daily EOD price refresh."""

    trade_date: date | None = None
    provider_exchange_codes: list[str] | None = None
    run_dbt_build: bool = False


@dataclass(frozen=True, slots=True)
class EodPriceBackfillRequest:
    """Inputs for one historical EOD price backfill."""

    from_date: date | None = None
    to_date: date | None = None
    provider_exchange_codes: list[str] | None = None
    batch_size: int = 50
    max_provider_calls: int | None = None
    build_selection_views_if_missing: bool = False
    run_dbt_build: bool = False


@dataclass(frozen=True, slots=True)
class EodPriceRefreshResult:
    """Completed EOD price refresh outcome."""

    run_id: str
    status: RunStatus
    summary: dict[str, object]


__all__ = ["EodPriceBackfillRequest", "EodPriceDailyRequest", "EodPriceRefreshResult"]
