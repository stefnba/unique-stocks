from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, model_validator


class EODBar(BaseModel):
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

    def to_bronze_record(self, provider: str = "eodhd") -> dict:
        """Serialise to a dict suitable for writing to bronze.eod_prices."""
        import hashlib
        import json

        payload = {
            "ticker": self.ticker,
            "bar_date": self.bar_date.isoformat(),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": self.volume,
            "adjusted_close": str(self.adjusted_close) if self.adjusted_close else None,
        }
        raw_json = json.dumps(payload, sort_keys=True)
        row_hash = hashlib.sha256(raw_json.encode()).hexdigest()

        return {
            "ticker": self.ticker,
            "bar_date": self.bar_date,
            "provider": provider,
            "raw_json": raw_json,
            "row_hash": row_hash,
        }
