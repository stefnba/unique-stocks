"""Infrastructure-only S3 key builder for pipeline storage.

``S3Key`` knows how to format canonical object paths. It does not know which
domain should use which layout; that policy lives in ``core.ingestion`` dataset
specs. ``S3Domain`` means storage dataset name, not a Python package under
``domains/``.

from core.clients.storage.s3 import S3Domain, S3Key
from providers.registry import Provider

# Snapshot — timestamped by UTC minute (exchange, instrument)
S3Key.snapshot(Provider.EODHD, S3Domain.EXCHANGE).jsonl()
# → "landing/eodhd/exchange/ingested_at=2026-05-21T09-47-32Z/exchange.jsonl"

# Partitioned (eod_price by exchange + date)
S3Key.partitioned(Provider.EODHD, S3Domain.EOD_PRICE, exchange="US", bar_date=date(2026, 5, 21)).jsonl()
# → "landing/eodhd/eod_price/exchange=US/bar_date=2026-05-21/data.jsonl"

# Bronze layer
S3Key.snapshot(Provider.EODHD, S3Domain.EXCHANGE, layer="bronze").jsonl()
# → "bronze/eodhd/exchange/ingested_at=2026-05-21T09-47-32Z/exchange.jsonl"
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Literal, Self

type LandingLayer = Literal["landing", "bronze"]


class S3Domain(StrEnum):
    """Canonical storage dataset names used in object keys.

    Keep these values stable because they become S3 path segments. Pipeline
    domain packages can share an S3 domain or write multiple Bronze datasets
    from one S3 domain when that matches the provider payload.
    """

    EXCHANGE = "exchange"
    EXCHANGE_SCHEDULE = "exchange_schedule"
    EOD_PRICE = "eod_price"
    FUNDAMENTAL = "fundamental"
    INSTRUMENT = "instrument"


def _utc_now_stamp() -> str:
    """Return current UTC datetime as a path-safe string, e.g. ``2026-05-21T09-47-32Z``."""
    now = datetime.now(UTC).replace(microsecond=0)
    return now.strftime("%Y-%m-%dT%H-%M-%SZ")


def _partition_value(value: date | datetime | str | int) -> str:
    """Serialize a partition value to a stable, path-safe string."""
    if isinstance(value, datetime):
        return value.replace(microsecond=0).strftime("%Y-%m-%dT%H-%M-%SZ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


@dataclass(frozen=True, slots=True)
class S3Key:
    """Constructs canonical, Hive-compatible S3 object keys for the data pipeline.

    ``provider`` accepts any plain string-like provider value.
    ``domain`` is constrained to :class:`S3Domain` to catch typos at type-check time.
    Partition key=value segments follow Hive convention so keys are directly
    queryable via DuckDB glob or AWS Athena.

    Prefer using ``LandingSpec.key`` from ``core.ingestion`` in domain code.
    Direct ``S3Key`` usage belongs in storage infrastructure and tests.
    """

    provider: str
    domain: S3Domain
    partitions: dict[str, str] = field(default_factory=dict)
    layer: LandingLayer = "landing"
    filename: str | None = None

    @classmethod
    def snapshot(
        cls,
        provider: str,
        domain: S3Domain,
        *,
        ingested_at: datetime | date | str | None = None,
        layer: LandingLayer = "landing",
    ) -> Self:
        """Full-replacement snapshot (e.g. exchange, instrument).

        Always includes an ``ingested_at`` partition for audit trail and
        idempotent re-runs. Defaults to the current UTC minute
        (e.g. ``2026-05-21T09-47-32Z``). Pass an explicit value for backfills:

        - ``datetime`` → formatted as ``YYYY-MM-DDTHH-MM-SSZ``
        - ``date`` → formatted as ``YYYY-MM-DD``
        - ``str`` → used as-is
        """
        if ingested_at is None:
            stamp = _utc_now_stamp()
        elif isinstance(ingested_at, datetime):
            stamp = ingested_at.strftime("%Y-%m-%dT%H-%M-%SZ")
        elif isinstance(ingested_at, date):
            stamp = ingested_at.isoformat()
        else:
            stamp = ingested_at
        return cls(
            provider=provider,
            domain=domain,
            partitions={"ingested_at": stamp},
            layer=layer,
            filename=str(domain),
        )

    @classmethod
    def partitioned(
        cls,
        provider: str,
        domain: S3Domain,
        *,
        layer: LandingLayer = "landing",
        **partition_kwargs: date | datetime | str | int,
    ) -> Self:
        """Partition by arbitrary kwargs (exchange, bar_date, ticker, …).

        :class:`datetime.date` values are serialized to ISO-8601 automatically.
        Partition order in the key mirrors the order kwargs are passed.
        """
        partitions = {k: _partition_value(v) for k, v in partition_kwargs.items()}
        return cls(provider=provider, domain=domain, partitions=partitions, layer=layer)

    @classmethod
    def partitioned_from_mapping(
        cls,
        provider: str,
        domain: S3Domain,
        partitions: Mapping[str, date | datetime | str | int],
        *,
        layer: LandingLayer = "landing",
    ) -> Self:
        """Partition by a dynamic mapping of key/value pairs.

        Dataset specs use this when their required partition fields are stored
        as data instead of static keyword arguments.
        """
        serialized = {key: _partition_value(value) for key, value in partitions.items()}
        return cls(provider=provider, domain=domain, partitions=serialized, layer=layer)

    def key(self, suffix: str) -> str:
        """Return the full S3 key with *suffix* as the file extension.

        Snapshot keys use ``{domain}.{suffix}`` as the filename.
        Partitioned keys use ``data.{suffix}`` — the path carries partition info.
        """
        ext = suffix.lstrip(".")
        filename = f"{self.filename}.{ext}" if self.filename else f"data.{ext}"
        parts = [self.layer, self.provider, self.domain]
        parts += [f"{k}={v}" for k, v in self.partitions.items()]
        return "/".join(parts) + f"/{filename}"

    def jsonl(self) -> str:
        """Return this key with a ``.jsonl`` suffix."""
        return self.key("jsonl")

    def json(self) -> str:
        """Return this key with a ``.json`` suffix."""
        return self.key("json")

    def csv(self) -> str:
        """Return this key with a ``.csv`` suffix."""
        return self.key("csv")


__all__ = ["LandingLayer", "S3Domain", "S3Key"]
