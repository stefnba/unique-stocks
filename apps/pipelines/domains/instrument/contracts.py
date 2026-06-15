"""Request and result contracts for instrument ingestion."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import RunStatus


@dataclass(frozen=True, slots=True)
class InstrumentRefreshRequest:
    """Inputs for one instrument refresh run."""

    snapshot_date: date | None = None
    provider_exchange_codes: list[str] | None = None


@dataclass(frozen=True, slots=True)
class InstrumentRefreshResult:
    """Completed instrument refresh outcome."""

    run_id: str
    status: RunStatus
    summary: dict[str, object]


__all__ = ["InstrumentRefreshRequest", "InstrumentRefreshResult"]
