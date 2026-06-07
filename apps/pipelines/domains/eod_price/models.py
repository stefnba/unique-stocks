from datetime import date
from decimal import Decimal
from typing import Self

from pydantic import ConfigDict, field_validator, model_validator

from core.models import BronzeModel

EOD_PRICE_INGESTION_MODES = {"daily_bulk", "historical_backfill"}


class EODBar(BronzeModel):
    """End-of-day price bar for a single provider instrument.

    A "bar" is the standard trading term for one OHLCV data point covering a
    time period — in this case a full trading day. Fields follow the
    conventional OHLCV layout used across financial data providers.

    Attributes:
        provider_exchange_code: Provider-specific exchange/catalog code used
            by endpoint paths and API symbol suffixes (for example ``US``).
        provider_instrument_code: Provider instrument code without exchange suffix,
            e.g. ``AAPL``.
        bar_date: The trading date this bar covers (NYSE session date).
        ingestion_mode: Pipeline mode that produced the bar. This is lineage
            only and is not part of price identity or idempotency.
        open: First traded price of the session.
        high: Highest traded price of the session.
        low: Lowest traded price of the session.
        close: Last traded price of the session (unadjusted).
        volume: Number of shares traded during the session.
        adjusted_close: Close price adjusted for splits and dividends.
            ``None`` when the provider does not supply it.

    Validation:
        Enforces the OHLC invariant on construction:
        ``low <= open <= high`` and ``low <= close <= high``.
        Any bar violating this is rejected before it can reach the lake.
    """

    model_config = ConfigDict(strict=True)

    provider_exchange_code: str
    provider_instrument_code: str
    bar_date: date
    ingestion_mode: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    adjusted_close: Decimal | None = None

    @field_validator("ingestion_mode")
    @classmethod
    def validate_ingestion_mode(cls, value: str) -> str:
        """Validate the supported EOD price ingestion mode values."""
        if value not in EOD_PRICE_INGESTION_MODES:
            allowed = ", ".join(sorted(EOD_PRICE_INGESTION_MODES))
            raise ValueError(f"Unsupported EOD price ingestion_mode {value!r}; expected one of: {allowed}")
        return value

    @model_validator(mode="after")
    def validate_ohlc(self) -> Self:
        """Validate OHLC price ordering invariants."""
        if not (self.low <= self.open <= self.high):
            raise ValueError(
                f"OHLC invariant violated for {self.provider_instrument_code} on {self.bar_date}: "
                f"open={self.open} not in [{self.low}, {self.high}]"
            )
        if not (self.low <= self.close <= self.high):
            raise ValueError(
                f"Close outside high/low for {self.provider_instrument_code} on {self.bar_date}: "
                f"close={self.close} not in [{self.low}, {self.high}]"
            )
        return self
