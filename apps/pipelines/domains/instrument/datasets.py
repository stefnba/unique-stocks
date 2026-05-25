"""Bronze dataset specs for instrument reference data."""

from dataclasses import dataclass
from datetime import date

from core.ingestion import BronzeDataset, LandingDomain, LandingTarget
from core.ingestion.landing import PartitionedLandingTarget
from core.ingestion.partitioning import LandingPartitionSchema
from domains.instrument.tables import INSTRUMENT_TABLE
from providers.registry import Provider


class InstrumentLandingPartition(LandingPartitionSchema):
    """Instrument landing partitions."""

    exchange: str
    snapshot_date: date


INSTRUMENT_LANDING = LandingTarget.partitioned(
    domain=LandingDomain.INSTRUMENT,
    partition_fields=InstrumentLandingPartition,
    file_format="jsonl",
)


@dataclass(frozen=True, slots=True)
class InstrumentLandings:
    """Landing targets that can produce ``bronze.instrument`` rows."""

    catalog: PartitionedLandingTarget[InstrumentLandingPartition]


INSTRUMENT_DATASET = BronzeDataset(
    provider=Provider.EODHD,
    table=INSTRUMENT_TABLE,
    landings=InstrumentLandings(catalog=INSTRUMENT_LANDING),
)

__all__ = [
    "INSTRUMENT_DATASET",
    "INSTRUMENT_LANDING",
    "InstrumentLandingPartition",
    "InstrumentLandings",
]
