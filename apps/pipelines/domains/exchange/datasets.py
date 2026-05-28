"""Bronze dataset specs for exchange reference data."""

from dataclasses import dataclass

from core.ingestion import BronzeDataset, LandingDomain, LandingTarget
from core.ingestion.landing import SnapshotLandingTarget
from domains.exchange.tables import EXCHANGE_CATALOG_TABLE, EXCHANGE_MIC_REGISTRY_TABLE
from providers.registry import Provider

EXCHANGE_CATALOG_LANDING = LandingTarget.snapshot(
    domain=LandingDomain.EXCHANGE,
    file_format="jsonl",
    audit_dataset="exchange.catalog",
)
EXCHANGE_MIC_REGISTRY_LANDING = LandingTarget.snapshot(
    domain=LandingDomain.EXCHANGE,
    file_format="csv",
    audit_dataset="exchange.mic_registry",
)


@dataclass(frozen=True, slots=True)
class ExchangeCatalogLandings:
    """Landing targets that can produce ``bronze.exchange_catalog`` rows.

    Attributes:
        catalog: Full exchange catalog snapshot landing target.
    """

    catalog: SnapshotLandingTarget


@dataclass(frozen=True, slots=True)
class ExchangeMicRegistryLandings:
    """Landing targets that can produce ``bronze.exchange_mic_registry`` rows."""

    mic_registry: SnapshotLandingTarget


EXCHANGE_CATALOG_DATASET = BronzeDataset(
    provider=Provider.EODHD,
    table=EXCHANGE_CATALOG_TABLE,
    landings=ExchangeCatalogLandings(catalog=EXCHANGE_CATALOG_LANDING),
)
EXCHANGE_MIC_REGISTRY_DATASET = BronzeDataset(
    provider=Provider.ISO10383,
    table=EXCHANGE_MIC_REGISTRY_TABLE,
    landings=ExchangeMicRegistryLandings(mic_registry=EXCHANGE_MIC_REGISTRY_LANDING),
)

__all__ = [
    "EXCHANGE_CATALOG_DATASET",
    "EXCHANGE_CATALOG_LANDING",
    "EXCHANGE_MIC_REGISTRY_DATASET",
    "EXCHANGE_MIC_REGISTRY_LANDING",
    "ExchangeCatalogLandings",
    "ExchangeMicRegistryLandings",
]
