"""S3 key builder for the data pipeline.

Single-import ergonomics — enums are accessible via ``S3Key.Provider`` and
``S3Key.Domain`` so callers only need one import::

    from core.clients.storage.s3 import S3Key

    # Snapshot — no partition (exchanges, securities)
    S3Key.snapshot(S3Key.Provider.EODHD, S3Key.Domain.EXCHANGES).jsonl()
    # → "landing/eodhd/exchanges/exchanges.jsonl"

    # Partitioned (eod_prices by exchange + date)
    S3Key.partitioned(S3Key.Provider.EODHD, S3Key.Domain.EOD_PRICES, exchange="US", bar_date=date(2026, 5, 21)).jsonl()
    # → "landing/eodhd/eod_prices/exchange=US/bar_date=2026-05-21/data.jsonl"

    # Bronze layer
    S3Key.snapshot(S3Key.Provider.EODHD, S3Key.Domain.EXCHANGES, layer="bronze").jsonl()
    # → "bronze/eodhd/exchanges/exchanges.jsonl"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
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
        layer: LandingLayer = "landing",
    ) -> Self:
        """Full-replacement snapshot — no partition (e.g. exchanges, securities)."""
        return cls(provider=provider, domain=domain, layer=layer)

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

    # ------------------------------------------------------------------
    # Key construction
    # ------------------------------------------------------------------

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
