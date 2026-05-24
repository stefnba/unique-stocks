"""Tests for central Bronze ingestion helpers."""

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from core.clients.storage.s3 import S3Domain
from core.ingestion import BronzeSource, LandingSpec, attach_source_uri, bronze_record, normalized_row_hash
from core.models import ProviderModel
from domains.eod_price.datasets import EOD_PRICE_BACKFILL_LANDING, EOD_PRICE_DAILY_LANDING, EOD_PRICE_DATASET
from domains.eod_price.models import EODBar
from providers.registry import Provider


def _bar(close: str = "190.75") -> EODBar:
    return EODBar(
        exchange_code="US",
        ticker="AAPL.US",
        bar_date=date(2026, 5, 9),
        open=Decimal("189.50"),
        high=Decimal("191.20"),
        low=Decimal("188.00"),
        close=Decimal(close),
        volume=55_000_000,
    )


def test_provider_model_has_no_bronze_serializer() -> None:
    """Provider models validate API contracts only."""
    assert not hasattr(ProviderModel, "to_bronze_record")


def test_provider_modules_do_not_import_bronze_policy() -> None:
    """Provider modules stay independent from Bronze serialization and writes."""
    provider_root = Path(__file__).parents[2] / "providers"
    forbidden_tokens = ("to_bronze_record", "BronzeModel", "bronze_record", "write_bronze")

    offenders: list[str] = []
    for path in provider_root.rglob("*.py"):
        text = path.read_text()
        for token in forbidden_tokens:
            if token in text:
                offenders.append(f"{path.relative_to(provider_root)}:{token}")

    assert offenders == []


def test_bronze_record_uses_provider_raw_json_and_normalized_hash() -> None:
    """raw_json comes from provider source; row_hash comes from normalized row."""
    raw = {"code": "AAPL", "unexpected_if_provider_allowed": "still raw fragment"}
    source = BronzeSource(row=_bar(), raw_fragment=raw, source_uri="s3://landing/eod.jsonl")

    record = bronze_record(EOD_PRICE_DATASET, source)

    assert json.loads(record["raw_json"]) == raw
    assert record["row_hash"] == normalized_row_hash(source.row.to_payload(), "eodhd")
    assert record["source_uri"] == "s3://landing/eod.jsonl"


def test_row_hash_ignores_raw_fragment() -> None:
    """Changing provider raw details does not change normalized-row hash."""
    first = bronze_record(EOD_PRICE_DATASET, BronzeSource(row=_bar(), raw_fragment={"code": "AAPL"}))
    second = bronze_record(EOD_PRICE_DATASET, BronzeSource(row=_bar(), raw_fragment={"code": "AAPL", "x": 1}))
    assert first["row_hash"] == second["row_hash"]


def test_attach_source_uri() -> None:
    """A landing URI can be attached to parser output after the landing write."""
    sources = [BronzeSource(row=_bar(), raw_fragment={"code": "AAPL"})]
    attached = attach_source_uri(sources, "s3://bucket/path.jsonl")
    assert attached[0].source_uri == "s3://bucket/path.jsonl"


def test_snapshot_landing_key() -> None:
    """Snapshot specs use domain filenames and an ingested_at partition."""
    spec = LandingSpec(S3Domain.EXCHANGE, style="snapshot", file_format="jsonl")
    assert spec.key(Provider.EODHD, ingested_at=date(2026, 5, 24)) == (
        "landing/eodhd/exchange/ingested_at=2026-05-24/exchange.jsonl"
    )


def test_partitioned_landing_key() -> None:
    """Partitioned specs use declared fields plus path-safe ingested_at."""
    key = EOD_PRICE_DAILY_LANDING.key(
        Provider.EODHD,
        exchange="US",
        bar_date=date(2026, 5, 24),
        ingested_at=datetime(2026, 5, 24, 12, 30),
    )
    assert key == (
        "landing/eodhd/eod_price/exchange=US/bar_date=2026-05-24/ingested_at=2026-05-24T12-30-00Z/data.jsonl"
    )


def test_backfill_landing_key() -> None:
    """Backfill landing keys include exchange, ticker, and requested date range."""
    key = EOD_PRICE_BACKFILL_LANDING.key(
        Provider.EODHD,
        exchange="US",
        ticker="AAPL.US",
        from_date=date(2020, 1, 1),
        to_date=date(2026, 5, 24),
        ingested_at=date(2026, 5, 24),
    )
    assert key == (
        "landing/eodhd/eod_price/exchange=US/ticker=AAPL.US/from_date=2020-01-01/"
        "to_date=2026-05-24/ingested_at=2026-05-24/data.jsonl"
    )
