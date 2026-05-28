"""Bronze dataset specs for exchange reference data."""

from dataclasses import dataclass

from core.ingestion import BronzeDataset, LandingDomain, LandingTarget
from core.ingestion.landing import SnapshotLandingTarget
from domains.exchange.tables import EXCHANGE_TABLE
from providers.registry import Provider

EXCHANGE_LANDING = LandingTarget.snapshot(
    domain=LandingDomain.EXCHANGE,
    file_format="jsonl",
    audit_dataset="exchange.catalog",
)


@dataclass(frozen=True, slots=True)
class ExchangeLandings:
    """Landing targets that can produce ``bronze.exchange`` rows."""

    catalog: SnapshotLandingTarget


EXCHANGE_DATASET = BronzeDataset(
    provider=Provider.EODHD,
    table=EXCHANGE_TABLE,
    landings=ExchangeLandings(catalog=EXCHANGE_LANDING),
)

__all__ = ["EXCHANGE_DATASET", "EXCHANGE_LANDING", "ExchangeLandings"]
