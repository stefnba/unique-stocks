"""Tests for EOD price parser output."""

import json
from datetime import date
from decimal import Decimal
from typing import Any

from core.ingestion import BronzeParseResult
from domains.eod_price.datasets import EOD_PRICE_DATASET
from domains.eod_price.models import EODBar
from domains.eod_price.parsers import infer_bulk_bar_date, parse_eod_bars, parse_instrument_bars
from providers.eodhd.models import EODBulkPriceRaw, EODPriceBarRaw

TARGET_DATE = date(2026, 5, 9)
EXCHANGE = "US"


def _row(**kwargs: Any) -> EODBulkPriceRaw:
    """Build an EODBulkPriceRaw using provider raw field names."""
    base: dict[str, Any] = {
        "code": "AAPL",
        "date": "2026-05-09",
        "open": 189.50,
        "high": 191.20,
        "low": 188.00,
        "close": 190.75,
        "volume": 55_000_000,
        "adjusted_close": 190.75,
    }
    return EODBulkPriceRaw(**{**base, **kwargs})


def _instrument_row(**kwargs: Any) -> EODPriceBarRaw:
    """Build an EODPriceBarRaw without the bulk endpoint's code field."""
    base: dict[str, Any] = {
        "date": "2026-05-09",
        "open": 189.50,
        "high": 191.20,
        "low": 188.00,
        "close": 190.75,
        "volume": 55_000_000,
        "adjusted_close": 190.75,
    }
    return EODPriceBarRaw(**{**base, **kwargs})


class TestParseEodBars:
    """Tests for parse_eod_bars validation and transformation logic."""

    def test_valid_row(self) -> None:
        """Valid row passes through with correct provider instrument and close price."""
        valid, rejected = parse_eod_bars([_row()], TARGET_DATE, EXCHANGE)
        assert len(valid) == 1
        assert len(rejected) == 0
        bar = valid[0].row
        assert bar.provider_exchange_code == "US"
        assert bar.provider_instrument_code == "AAPL"
        assert bar.ingestion_path == "daily_bulk"
        assert bar.close == Decimal("190.75")
        assert valid[0].raw_fragment.code == "AAPL"

    def test_ohlc_invariant_open_above_high_rejected(self) -> None:
        """Row where open > high violates the OHLC invariant and is rejected."""
        row = _row(open=195.00, high=191.20)
        valid, rejected = parse_eod_bars([row], TARGET_DATE, EXCHANGE)
        assert len(valid) == 0
        assert len(rejected) == 1
        assert rejected[0].code == "AAPL"

    def test_close_outside_range_rejected(self) -> None:
        """Row where close > high is rejected."""
        row = _row(close=195.00)
        valid, rejected = parse_eod_bars([row], TARGET_DATE, EXCHANGE)
        assert len(valid) == 0
        assert len(rejected) == 1

    def test_date_mismatch_silently_dropped(self) -> None:
        """Rows for a different date are dropped, not rejected."""
        row = _row(date="2026-05-08")
        valid, rejected = parse_eod_bars([row], TARGET_DATE, EXCHANGE)
        assert len(valid) == 0
        assert len(rejected) == 0

    def test_mixed_valid_and_invalid(self) -> None:
        """Valid and invalid rows are split correctly."""
        rows = [
            _row(code="AAPL"),
            _row(code="MSFT", open=999.00),
            _row(code="GOOG"),
        ]
        valid, rejected = parse_eod_bars(rows, TARGET_DATE, EXCHANGE)
        assert len(valid) == 2
        assert len(rejected) == 1

    def test_zero_volume_is_accepted_and_preserved(self) -> None:
        """Zero volume is accepted and preserved."""
        row = _row(volume=0)
        valid, rejected = parse_eod_bars([row], TARGET_DATE, EXCHANGE)
        assert len(rejected) == 0
        assert len(valid) == 1
        assert valid[0].row.volume == 0

    def test_no_adjusted_close(self) -> None:
        """Missing adjusted_close is preserved as None."""
        row = _row(adjusted_close=None)
        valid, rejected = parse_eod_bars([row], TARGET_DATE, EXCHANGE)
        assert len(rejected) == 0
        assert len(valid) == 1
        assert valid[0].row.adjusted_close is None

    def test_provider_instrument_code_is_not_exchange_qualified(self) -> None:
        """Bulk parsing stores provider instrument code separately from exchange code."""
        row = _row(code="TSLA")
        valid, _ = parse_eod_bars([row], TARGET_DATE, "NASDAQ")
        assert valid[0].row.provider_exchange_code == "NASDAQ"
        assert valid[0].row.provider_instrument_code == "TSLA"

    def test_bulk_bar_date_infers_latest_provider_date(self) -> None:
        """Daily runs without an explicit date use the latest returned provider date."""
        rows = [_row(date="2026-05-08"), _row(date="2026-05-09")]
        assert infer_bulk_bar_date(rows) == TARGET_DATE


class TestParseInstrumentBars:
    """Tests for per-instrument backfill parser behavior."""

    def test_valid_row_uses_split_instrument_context(self) -> None:
        """Per-instrument rows get exchange and instrument code from the caller."""
        raw = _instrument_row()
        valid, rejected = parse_instrument_bars([raw], provider_exchange_code="US", provider_instrument_code="AAPL")

        assert rejected == []
        assert len(valid) == 1
        assert valid[0].row.provider_exchange_code == "US"
        assert valid[0].row.provider_instrument_code == "AAPL"
        assert valid[0].row.ingestion_path == "historical_backfill"
        assert valid[0].row.close == Decimal("190.75")
        assert valid[0].raw_fragment is raw

    def test_ohlc_invariant_rejected(self) -> None:
        """Per-instrument rows use the same OHLC validation as bulk rows."""
        raw = _instrument_row(close=195.00)
        valid, rejected = parse_instrument_bars([raw], provider_exchange_code="US", provider_instrument_code="AAPL")

        assert valid == []
        assert rejected == [raw]

    def test_dates_are_not_filtered_by_daily_target(self) -> None:
        """Historical backfills keep every provider row in the requested range."""
        rows = [_instrument_row(date="2026-05-08"), _instrument_row(date="2026-05-09")]
        valid, rejected = parse_instrument_bars(rows, provider_exchange_code="US", provider_instrument_code="AAPL")

        assert rejected == []
        assert [source.row.bar_date for source in valid] == [date(2026, 5, 8), date(2026, 5, 9)]


class TestEodBarBronzeRecord:
    """Tests for EODBar Bronze serialization through the new dataset API."""

    def test_record_has_row_hash(self) -> None:
        """Bronze record includes a 64-char SHA-256 row_hash."""
        bar = EODBar(
            provider_exchange_code="US",
            provider_instrument_code="AAPL",
            bar_date=date(2026, 5, 9),
            ingestion_path="daily_bulk",
            open=Decimal("189.50"),
            high=Decimal("191.20"),
            low=Decimal("188.00"),
            close=Decimal("190.75"),
            volume=55_000_000,
        )
        source = BronzeParseResult(row=bar, raw_fragment=_row(), source_uri="s3://bucket/key.jsonl")
        record = EOD_PRICE_DATASET.bronze_record(source)
        assert "row_hash" in record
        assert len(record["row_hash"]) == 64
        assert record["data_provider"] == "eodhd"
        assert record["source_uri"] == "s3://bucket/key.jsonl"
        assert json.loads(record["raw_json"])["code"] == "AAPL"

    def test_record_is_deterministic(self) -> None:
        """Same bar always produces the same row_hash."""
        bar = EODBar(
            provider_exchange_code="US",
            provider_instrument_code="AAPL",
            bar_date=date(2026, 5, 9),
            ingestion_path="daily_bulk",
            open=Decimal("189.50"),
            high=Decimal("191.20"),
            low=Decimal("188.00"),
            close=Decimal("190.75"),
            volume=55_000_000,
        )
        source = BronzeParseResult(row=bar, raw_fragment=_row())
        first = EOD_PRICE_DATASET.bronze_record(source)
        second = EOD_PRICE_DATASET.bronze_record(source)
        assert first["row_hash"] == second["row_hash"]
