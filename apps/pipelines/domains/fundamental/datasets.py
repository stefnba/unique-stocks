"""Bronze dataset specs for fundamentals ingestion."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import BronzeDataset, LandingDomain, LandingTarget
from core.ingestion.landing import PartitionedLandingTarget
from core.ingestion.partitioning import LandingPartitionSchema
from domains.fundamental.tables import (
    FUNDAMENTAL_DOCUMENT_TABLE,
    FUNDAMENTAL_STATEMENT_FACT_TABLE,
    FUNDAMENTAL_STOCK_IDENTITY_TABLE,
)
from providers.registry import Provider


class FundamentalDocumentPartition(LandingPartitionSchema):
    """Fundamentals document landing partitions."""

    provider_exchange_code: str
    ticker: str
    snapshot_date: date


FUNDAMENTAL_DOCUMENT_LANDING = LandingTarget.partitioned(
    domain=LandingDomain.FUNDAMENTAL,
    partition_fields=FundamentalDocumentPartition,
    file_format="json",
)


@dataclass(frozen=True, slots=True)
class FundamentalLandings:
    """Landing targets that can produce fundamentals Bronze rows."""

    document: PartitionedLandingTarget[FundamentalDocumentPartition]


FUNDAMENTAL_LANDINGS = FundamentalLandings(document=FUNDAMENTAL_DOCUMENT_LANDING)

FUNDAMENTAL_DOCUMENT_DATASET = BronzeDataset(
    provider=Provider.EODHD,
    table=FUNDAMENTAL_DOCUMENT_TABLE,
    landings=FUNDAMENTAL_LANDINGS,
)

FUNDAMENTAL_STOCK_IDENTITY_DATASET = BronzeDataset(
    provider=Provider.EODHD,
    table=FUNDAMENTAL_STOCK_IDENTITY_TABLE,
    landings=FUNDAMENTAL_LANDINGS,
)

FUNDAMENTAL_STATEMENT_FACT_DATASET = BronzeDataset(
    provider=Provider.EODHD,
    table=FUNDAMENTAL_STATEMENT_FACT_TABLE,
    landings=FUNDAMENTAL_LANDINGS,
)

__all__ = [
    "FUNDAMENTAL_DOCUMENT_DATASET",
    "FUNDAMENTAL_DOCUMENT_LANDING",
    "FUNDAMENTAL_LANDINGS",
    "FUNDAMENTAL_STATEMENT_FACT_DATASET",
    "FUNDAMENTAL_STOCK_IDENTITY_DATASET",
    "FundamentalDocumentPartition",
    "FundamentalLandings",
]
