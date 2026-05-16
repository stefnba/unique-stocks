from datetime import date
from decimal import Decimal
from typing import Any

from domains.eod_prices.models import EODBar
from domains.eod_prices.parsers import parse_eod_bars
from providers.eodhd.models import EODBulkPriceRaw

TARGET_DATE = date(2026, 5, 9)
EXCHANGE = "US"


def _row(**kwargs: Any) -> EODBulkPriceRaw:
    """Build an EODBulkPriceRaw using EODHD's raw field names (code, date)."""
    base: dict[str, Any] = {
        "code": "AAPL",  # exchange suffix added by parse_eod_bars
        "date": "2026-05-09",
        "open": 189.50,
        "high": 191.20,
        "low": 188.00,
        "close": 190.75,
        "volume": 55_000_000,
        "adjusted_close": 190.75,
    }
    return EODBulkPriceRaw(**{**base, **kwargs})


class TestParseEodBars:
    """Tests for parse_eod_bars validation and transformation logic."""

    def test_valid_row(self):
        """Valid row passes through with correct ticker and close price."""
        valid, rejected = parse_eod_bars([_row()], TARGET_DATE, EXCHANGE)
        assert len(valid) == 1
        assert len(rejected) == 0
        bar = valid[0]
        assert bar.ticker == "AAPL.US"  # code + exchange suffix
        assert bar.close == Decimal("190.75")

    def test_ohlc_invariant_open_above_high_rejected(self):
        """Row where open > high violates the OHLC invariant and is rejected."""
        row = _row(open=195.00, high=191.20)  # open > high
        valid, rejected = parse_eod_bars([row], TARGET_DATE, EXCHANGE)
        assert len(valid) == 0
        assert len(rejected) == 1
        assert rejected[0].code == "AAPL"

    def test_close_outside_range_rejected(self):
        """Row where close > high is rejected."""
        row = _row(close=195.00)  # close > high
        valid, rejected = parse_eod_bars([row], TARGET_DATE, EXCHANGE)
        assert len(valid) == 0
        assert len(rejected) == 1

    def test_date_mismatch_silently_dropped(self):
        """Row with a date different from expected_date is silently dropped (not rejected)."""
        row = _row(date="2026-05-08")  # different date
        valid, rejected = parse_eod_bars([row], TARGET_DATE, EXCHANGE)
        assert len(valid) == 0
        assert len(rejected) == 0  # dropped, not rejected

    def test_mixed_valid_and_invalid(self):
        """Valid and invalid rows are split correctly."""
        rows = [
            _row(code="AAPL"),
            _row(code="MSFT", open=999.00),  # open > high → invalid
            _row(code="GOOG"),
        ]
        valid, rejected = parse_eod_bars(rows, TARGET_DATE, EXCHANGE)
        assert len(valid) == 2
        assert len(rejected) == 1

    def test_none_volume_defaults_to_zero(self):
        """Zero volume is accepted and preserved."""
        row = _row(volume=0)
        valid, rejected = parse_eod_bars([row], TARGET_DATE, EXCHANGE)
        assert len(valid) == 1
        assert valid[0].volume == 0

    def test_no_adjusted_close(self):
        """Missing adjusted_close is preserved as None."""
        row = _row(adjusted_close=None)
        valid, rejected = parse_eod_bars([row], TARGET_DATE, EXCHANGE)
        assert len(valid) == 1
        assert valid[0].adjusted_close is None

    def test_ticker_has_exchange_suffix(self):
        """Ticker is constructed as code.exchange."""
        row = _row(code="TSLA")
        valid, _ = parse_eod_bars([row], TARGET_DATE, "NASDAQ")
        assert valid[0].ticker == "TSLA.NASDAQ"


class TestEodBarBronzeRecord:
    """Tests for EODBar.to_bronze_record serialisation."""

    def test_record_has_row_hash(self):
        """Bronze record includes a 64-char SHA-256 row_hash."""
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
        """Same bar always produces the same row_hash."""
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
