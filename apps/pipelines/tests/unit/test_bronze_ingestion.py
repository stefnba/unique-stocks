"""Tests for central Bronze ingestion helpers."""

import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from core.clients.lake import DataLakeClient
from core.clients.storage.s3 import S3Domain, S3ObjectRef, S3StorageClient
from core.ingestion import (
    BronzeSource,
    LandingSpec,
    already_ingested,
    attach_source_uri,
    bronze_record,
    normalized_row_hash,
    save_landing,
    write_bronze,
)
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


class FakeS3:
    """Minimal S3 fake for save_landing contract tests."""

    saved_key: str | None = None
    saved_data: Any = None

    def save(self, key: str, data: Any) -> S3ObjectRef:
        """Capture the key and payload passed to storage."""
        self.saved_key = key
        self.saved_data = data
        return S3ObjectRef(bucket="landing-bucket", key=key)


class FakeLake:
    """Minimal lake fake for Bronze write and idempotency tests."""

    query_result: dict[str, Any] | None = None
    last_query: str | None = None
    last_params: Sequence[Any] | None = None
    inserted_schema: str | None = None
    inserted_table: str | None = None
    inserted_rows: list[dict[str, Any]]

    def __init__(self, query_result: dict[str, Any] | None = None) -> None:
        """Create the fake with an optional query result."""
        self.query_result = query_result
        self.inserted_rows = []

    def qualified_name(self, schema: str, table: str) -> str:
        """Return an unquoted qualified table name for assertions."""
        return f"{schema}.{table}"

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Capture the idempotency query and return the configured row."""
        self.last_query = sql
        self.last_params = params
        return self.query_result

    def insert_rows(self, schema: str, table: str, rows: Iterable[Mapping[str, Any]]) -> int:
        """Capture inserted rows and return their count."""
        self.inserted_schema = schema
        self.inserted_table = table
        self.inserted_rows = [dict(row) for row in rows]
        return len(self.inserted_rows)


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


def test_save_landing_serializes_provider_payload_with_landing_key() -> None:
    """Landing writes preserve provider field names and return the object URI."""
    s3 = FakeS3()
    raw = {"code": "AAPL", "date": "2026-05-09"}
    expected_key = "landing/eodhd/eod_price/exchange=US/bar_date=2026-05-09/ingested_at=2026-05-24/data.jsonl"

    ref = save_landing(
        cast(S3StorageClient, s3),
        EOD_PRICE_DAILY_LANDING,
        Provider.EODHD,
        raw,
        ingested_at=date(2026, 5, 24),
        exchange="US",
        bar_date=date(2026, 5, 9),
    )

    assert ref.uri == f"s3://landing-bucket/{expected_key}"
    assert s3.saved_key == expected_key
    assert s3.saved_data == raw


def test_write_bronze_inserts_serialized_rows_with_fallback_source_uri() -> None:
    """write_bronze serializes parser output and delegates one insert to the lake."""
    lake = FakeLake()
    source = BronzeSource(row=_bar(), raw_fragment={"code": "AAPL"})

    written = write_bronze(
        cast(DataLakeClient, lake),
        EOD_PRICE_DATASET,
        [source],
        source_uri="s3://landing/eod.jsonl",
    )

    assert written == 1
    assert lake.inserted_schema == "bronze"
    assert lake.inserted_table == "eod_price"
    assert lake.inserted_rows[0]["ticker"] == "AAPL.US"
    assert lake.inserted_rows[0]["source_uri"] == "s3://landing/eod.jsonl"
    assert json.loads(lake.inserted_rows[0]["raw_json"]) == {"code": "AAPL"}


def test_write_bronze_skips_empty_sources() -> None:
    """Empty Bronze writes do not call into the lake client."""
    lake = FakeLake()

    assert write_bronze(cast(DataLakeClient, lake), EOD_PRICE_DATASET, []) == 0
    assert lake.inserted_rows == []


def test_already_ingested_checks_idempotency_partition_and_provider() -> None:
    """Idempotency checks include declared table keys plus data_provider."""
    lake = FakeLake(query_result={"cnt": 1})

    exists = already_ingested(
        cast(DataLakeClient, lake),
        EOD_PRICE_DATASET,
        exchange_code="US",
        bar_date=date(2026, 5, 9),
    )

    assert exists is True
    assert lake.last_query == (
        "SELECT COUNT(*) AS cnt FROM bronze.eod_price WHERE exchange_code = ? AND bar_date = ? AND data_provider = ?"
    )
    assert lake.last_params == ["US", "2026-05-09", "eodhd"]


def test_already_ingested_requires_all_idempotency_values() -> None:
    """Missing partition values fail before a lake query is built."""
    lake = FakeLake()

    with pytest.raises(ValueError, match="bar_date"):
        already_ingested(cast(DataLakeClient, lake), EOD_PRICE_DATASET, exchange_code="US")


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


def test_partitioned_landing_key_requires_declared_fields() -> None:
    """Partitioned landing specs fail fast when a required field is omitted."""
    with pytest.raises(ValueError, match="bar_date"):
        EOD_PRICE_DAILY_LANDING.key(Provider.EODHD, exchange="US", ingested_at=date(2026, 5, 24))


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
