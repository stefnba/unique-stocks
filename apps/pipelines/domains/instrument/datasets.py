"""Bronze dataset specs for instrument reference data."""

from dataclasses import dataclass
from datetime import date

from config.domains import Domain
from config.providers import Provider
from core.ingestion import BronzeDataset, LandingTarget
from core.ingestion.landing import PartitionedLandingTarget
from core.ingestion.partitioning import LandingPartitionSchema
from domains.instrument.tables import INSTRUMENT_TABLE


class InstrumentLandingPartition(LandingPartitionSchema):
    """Instrument landing partitions.

    Attributes:
        provider_exchange_code: Provider exchange code requested from EODHD.
        snapshot_date: Logical catalog snapshot date.
    """

    provider_exchange_code: str
    snapshot_date: date


INSTRUMENT_LANDING = LandingTarget.partitioned(
    domain=Domain.INSTRUMENT,
    partition_fields=InstrumentLandingPartition,
    file_format="jsonl",
)


@dataclass(frozen=True, slots=True)
class InstrumentLandings:
    """Landing targets that can produce ``bronze.instrument`` rows.

    Attributes:
        catalog: Instrument catalog landing target.
    """

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
