"""Request and result contracts for fundamentals ingestion."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import RunStatus


@dataclass(frozen=True, slots=True)
class FundamentalRefreshRequest:
    """Inputs for one fundamentals refresh run."""

    provider_instruments: list[dict[str, str]] | None = None
    snapshot_date: date | None = None
    ingestion_batch_date: date | None = None
    continue_ingestion_batch: bool = False
    provider_exchange_codes: list[str] | None = None
    limit: int | None = None
    skip_existing: bool = True
    refresh_existing: bool = False
    replay_landing: bool = False
    landing_source_uris_by_instrument: dict[str, str] | None = None
    batch_size: int = 1
    provider_batch_delay_seconds: float = 0.0
    provider_credits_per_call: int = 10
    max_provider_credits: int | None = None


@dataclass(frozen=True, slots=True)
class FundamentalRefreshResult:
    """Completed fundamentals refresh outcome."""

    run_id: str
    status: RunStatus
    summary: dict[str, object]


__all__ = ["FundamentalRefreshRequest", "FundamentalRefreshResult"]
