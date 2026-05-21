"""S3 key builder for the data pipeline.

Single-import ergonomics — enums are accessible via ``S3Key.Provider`` and
``S3Key.Domain`` so callers only need one import::

    from core.clients.storage.s3 import S3Key

    # Snapshot — timestamped by UTC minute (exchanges, securities)
    S3Key.snapshot(S3Key.Provider.EODHD, S3Key.Domain.EXCHANGES).jsonl()
    # → "landing/eodhd/exchanges/ingested_at=2026-05-21T09-47-32Z/exchanges.jsonl"

    # Partitioned (eod_prices by exchange + date)
    S3Key.partitioned(S3Key.Provider.EODHD, S3Key.Domain.EOD_PRICES, exchange="US", bar_date=date(2026, 5, 21)).jsonl()
    # → "landing/eodhd/eod_prices/exchange=US/bar_date=2026-05-21/data.jsonl"

    # Bronze layer
    S3Key.snapshot(S3Key.Provider.EODHD, S3Key.Domain.EXCHANGES, layer="bronze").jsonl()
    # → "bronze/eodhd/exchanges/ingested_at=2026-05-21T09-47-32Z/exchanges.jsonl"
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import StrEnum
from typing import ClassVar, Literal, Self

from providers.registry import Provider as _Provider


type LandingLayer = Literal["landing", "bronze"]


class Domain(StrEnum):
    EXCHANGES = "exchanges"
    EOD_PRICES = "eod_prices"
    FUNDAMENTALS = "fundamentals"
    SECURITIES = "securities"


# Aliases to avoid name shadowing inside the S3Key class body.
_DomainCls = Domain
_ProviderCls = _Provider


def _utc_now_stamp() -> str:
    """Return current UTC datetime as a path-safe string, e.g. ``2026-05-21T09-47-32Z``."""
    now = datetime.now(timezone.utc).replace(microsecond=0)
    return now.strftime("%Y-%m-%dT%H-%M-%SZ")


@dataclass(frozen=True, slots=True)
class S3Key:
    """Constructs canonical, Hive-compatible S3 object keys for the data pipeline.

    ``provider`` accepts a :data:`S3Key.Provider` value or any plain string.
    ``domain`` is constrained to :data:`S3Key.Domain` to catch typos at type-check time.
    Partition key=value segments follow Hive convention so keys are directly
    queryable via DuckDB glob or AWS Athena.
    """

    Provider: ClassVar[type[_ProviderCls]] = _ProviderCls
    Domain: ClassVar[type[_DomainCls]] = _DomainCls

    provider: str
    domain: _DomainCls
    partitions: dict[str, str] = field(default_factory=dict)
    layer: LandingLayer = "landing"

    @classmethod
    def snapshot(
        cls,
        provider: str,
        domain: _DomainCls,
        *,
        ingested_at: datetime | date | str | None = None,
        layer: LandingLayer = "landing",
    ) -> Self:
        """Full-replacement snapshot (e.g. exchanges, securities).

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
        )

    @classmethod
    def partitioned(
        cls,
        provider: str,
        domain: _DomainCls,
        *,
        layer: LandingLayer = "landing",
        **partition_kwargs: date | str | int,
    ) -> Self:
        """Partition by arbitrary kwargs (exchange, bar_date, ticker, …).

        :class:`datetime.date` values are serialized to ISO-8601 automatically.
        Partition order in the key mirrors the order kwargs are passed.
        """
        partitions = {
            k: v.isoformat() if isinstance(v, date) else str(v)
            for k, v in partition_kwargs.items()
        }
        return cls(provider=provider, domain=domain, partitions=partitions, layer=layer)

    def key(self, suffix: str) -> str:
        """Return the full S3 key with *suffix* as the file extension.

        Snapshot keys use ``{domain}.{suffix}`` as the filename.
        Partitioned keys use ``data.{suffix}`` — the path carries partition info.
        """
        ext = suffix.lstrip(".")
        filename = f"data.{ext}" if self.partitions else f"{self.domain}.{ext}"
        parts = [self.layer, self.provider, self.domain]
        parts += [f"{k}={v}" for k, v in self.partitions.items()]
        return "/".join(parts) + f"/{filename}"

    def jsonl(self) -> str:
        return self.key("jsonl")

    def json(self) -> str:
        return self.key("json")

    def csv(self) -> str:
        return self.key("csv")


__all__ = ["Domain", "LandingLayer", "S3Key"]
