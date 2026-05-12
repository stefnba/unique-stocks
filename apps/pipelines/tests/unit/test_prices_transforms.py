from datetime import date
from decimal import Decimal

import pytest

from pipelines.prices.transforms import parse_eod_bars
from shared.schemas.prices import EODBar


def _row(**kwargs) -> dict:
    base = {
        "ticker": "AAPL.US",
        "bar_date": "2026-05-09",
        "open": "189.50",
        "high": "191.20",
        "low": "188.00",
        "close": "190.75",
        "volume": 55_000_000,
        "adjusted_close": "190.75",
    }
    return {**base, **kwargs}


TARGET_DATE = date(2026, 5, 9)


class TestParseEodBars:
    def test_valid_row(self):
        valid, rejected = parse_eod_bars([_row()], TARGET_DATE)
        assert len(valid) == 1
        assert len(rejected) == 0
        bar = valid[0]
        assert bar.ticker == "AAPL.US"
        assert bar.close == Decimal("190.75")

    def test_ohlc_invariant_open_above_high_rejected(self):
        row = _row(open="195.00", high="191.20")  # open > high
        valid, rejected = parse_eod_bars([row], TARGET_DATE)
        assert len(valid) == 0
        assert len(rejected) == 1
        assert "OHLC invariant" in rejected[0]["error"]

    def test_close_outside_range_rejected(self):
        row = _row(close="195.00")  # close > high
        valid, rejected = parse_eod_bars([row], TARGET_DATE)
        assert len(valid) == 0
        assert len(rejected) == 1

    def test_date_mismatch_silently_dropped(self):
        row = _row(bar_date="2026-05-08")  # different date
        valid, rejected = parse_eod_bars([row], TARGET_DATE)
        assert len(valid) == 0
        assert len(rejected) == 0  # dropped, not rejected

    def test_mixed_valid_and_invalid(self):
        rows = [
            _row(ticker="AAPL.US"),
            _row(ticker="MSFT.US", open="999.00"),  # open > high → invalid
            _row(ticker="GOOG.US"),
        ]
        valid, rejected = parse_eod_bars(rows, TARGET_DATE)
        assert len(valid) == 2
        assert len(rejected) == 1

    def test_none_volume_defaults_to_zero(self):
        row = _row(volume=None)
        valid, rejected = parse_eod_bars([row], TARGET_DATE)
        assert len(valid) == 1
        assert valid[0].volume == 0

    def test_no_adjusted_close(self):
        row = _row(adjusted_close=None)
        valid, rejected = parse_eod_bars([row], TARGET_DATE)
        assert len(valid) == 1
        assert valid[0].adjusted_close is None


class TestEodBarBronzeRecord:
    def test_record_has_row_hash(self):
        bar = EODBar(
            ticker="AAPL.US",
            bar_date=date(2026, 5, 9),
            open=Decimal("189.50"),
            high=Decimal("191.20"),
            low=Decimal("188.00"),
            close=Decimal("190.75"),
            volume=55_000_000,
        )
        record = bar.to_bronze_record()
        assert "row_hash" in record
        assert len(record["row_hash"]) == 64  # SHA-256 hex

    def test_record_is_deterministic(self):
        bar = EODBar(
            ticker="AAPL.US",
            bar_date=date(2026, 5, 9),
            open=Decimal("189.50"),
            high=Decimal("191.20"),
            low=Decimal("188.00"),
            close=Decimal("190.75"),
            volume=55_000_000,
        )
        assert bar.to_bronze_record()["row_hash"] == bar.to_bronze_record()["row_hash"]
