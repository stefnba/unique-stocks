from datetime import date
from decimal import Decimal

from pydantic import ConfigDict, model_validator

from core.models import BronzeModel


class EODBar(BronzeModel):
    """End-of-day price bar for a single ticker.

    A "bar" is the standard trading term for one OHLCV data point covering a
    time period — in this case a full trading day. Fields follow the
    conventional OHLCV layout used across financial data providers.

    Attributes:
        ticker: Exchange-qualified symbol, e.g. ``AAPL.US``.
        bar_date: The trading date this bar covers (NYSE session date).
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

    model_config = ConfigDict(strict=True, extra="forbid")

    ticker: str
    bar_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    adjusted_close: Decimal | None = None

    @model_validator(mode="after")
    def validate_ohlc(self) -> "EODBar":
        if not (self.low <= self.open <= self.high):
            raise ValueError(
                f"OHLC invariant violated for {self.ticker} on {self.bar_date}: "
                f"open={self.open} not in [{self.low}, {self.high}]"
            )
        if not (self.low <= self.close <= self.high):
            raise ValueError(
                f"Close outside high/low for {self.ticker} on {self.bar_date}: "
                f"close={self.close} not in [{self.low}, {self.high}]"
            )
        return self


