"""Bronze dataset specs for exchange reference data."""

from core.clients.storage.s3 import S3Domain
from core.ingestion import BronzeDataset, LandingSpec
from domains.exchanges.models import ExchangeSnapshot
from domains.exchanges.tables import EXCHANGES_TABLE
from providers.registry import Provider

EXCHANGES_LANDING = LandingSpec(
    s3_domain=S3Domain.EXCHANGES,
    style="snapshot",
    file_format="jsonl",
)

EXCHANGES_DATASET = BronzeDataset[ExchangeSnapshot](
    provider=Provider.EODHD,
    table=EXCHANGES_TABLE,
    landing=EXCHANGES_LANDING,
)

__all__ = ["EXCHANGES_DATASET", "EXCHANGES_LANDING"]
