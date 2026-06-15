"""Request and result contracts for exchange reference ingestion."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import RunStatus


@dataclass(frozen=True, slots=True)
class ExchangeCatalogRefreshRequest:
    """Inputs for one provider exchange catalog refresh."""

    snapshot_date: date | None = None


@dataclass(frozen=True, slots=True)
class ExchangeCatalogRefreshResult:
    """Completed provider exchange catalog refresh outcome."""

    run_id: str
    status: RunStatus
    summary: dict[str, object]
    rows_written: int


@dataclass(frozen=True, slots=True)
class ExchangeMicRegistryRefreshRequest:
    """Inputs for one ISO MIC registry refresh."""

    snapshot_date: date | None = None


@dataclass(frozen=True, slots=True)
class ExchangeMicRegistryRefreshResult:
    """Completed ISO MIC registry refresh outcome."""

    run_id: str
    status: RunStatus
    summary: dict[str, object]


__all__ = [
    "ExchangeCatalogRefreshRequest",
    "ExchangeCatalogRefreshResult",
    "ExchangeMicRegistryRefreshRequest",
    "ExchangeMicRegistryRefreshResult",
]
