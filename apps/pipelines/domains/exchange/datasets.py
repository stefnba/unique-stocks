"""Bronze dataset specs for exchange reference data."""

from core.clients.storage.s3 import S3Domain
from core.ingestion import BronzeDataset, LandingSpec
from domains.exchange.models import ExchangeSnapshot
from domains.exchange.tables import EXCHANGE_TABLE
from providers.registry import Provider

EXCHANGE_LANDING = LandingSpec(
    s3_domain=S3Domain.EXCHANGE,
    style="snapshot",
    file_format="jsonl",
)

EXCHANGE_DATASET = BronzeDataset[ExchangeSnapshot](
    provider=Provider.EODHD,
    table=EXCHANGE_TABLE,
    landing=EXCHANGE_LANDING,
)

__all__ = ["EXCHANGE_DATASET", "EXCHANGE_LANDING"]
