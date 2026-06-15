"""Request and result contracts for exchange schedule ingestion."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import RunStatus


@dataclass(frozen=True, slots=True)
class ExchangeScheduleRefreshRequest:
    """Inputs for one exchange schedule refresh run."""

    snapshot_date: date | None = None
    provider_schedule_exchange_codes: list[str] | None = None
    batch_size: int = 10
    provider_batch_delay_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class ExchangeScheduleRefreshResult:
    """Completed exchange schedule refresh outcome."""

    run_id: str
    status: RunStatus
    summary: dict[str, object]


__all__ = ["ExchangeScheduleRefreshRequest", "ExchangeScheduleRefreshResult"]
