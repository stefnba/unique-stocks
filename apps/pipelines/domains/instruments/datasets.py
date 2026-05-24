"""Bronze dataset specs for instrument reference data."""

from core.clients.storage.s3 import S3Domain
from core.ingestion import BronzeDataset, LandingSpec
from domains.instruments.models import InstrumentSnapshot
from domains.instruments.tables import INSTRUMENTS_TABLE
from providers.registry import Provider

INSTRUMENTS_LANDING = LandingSpec(
    s3_domain=S3Domain.INSTRUMENTS,
    style="partitioned",
    partition_fields=("exchange",),
    file_format="jsonl",
)

INSTRUMENTS_DATASET = BronzeDataset[InstrumentSnapshot](
    provider=Provider.EODHD,
    table=INSTRUMENTS_TABLE,
    landing=INSTRUMENTS_LANDING,
)

__all__ = ["INSTRUMENTS_DATASET", "INSTRUMENTS_LANDING"]
