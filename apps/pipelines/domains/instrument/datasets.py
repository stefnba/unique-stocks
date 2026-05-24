"""Bronze dataset specs for instrument reference data."""

from core.clients.storage.s3 import S3Domain
from core.ingestion import BronzeDataset, LandingSpec
from domains.instrument.models import InstrumentSnapshot
from domains.instrument.tables import INSTRUMENT_TABLE
from providers.registry import Provider

INSTRUMENT_LANDING = LandingSpec(
    s3_domain=S3Domain.INSTRUMENT,
    style="partitioned",
    partition_fields=("exchange",),
    file_format="jsonl",
)

INSTRUMENT_DATASET = BronzeDataset[InstrumentSnapshot](
    provider=Provider.EODHD,
    table=INSTRUMENT_TABLE,
    landing=INSTRUMENT_LANDING,
)

__all__ = ["INSTRUMENT_DATASET", "INSTRUMENT_LANDING"]
