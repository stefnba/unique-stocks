"""Tests for the new ingestion landing and Bronze dataset surfaces."""

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, ClassVar, cast

import pytest
from pydantic import Field

from core.clients.lake import DataLakeClient
from core.clients.storage.s3.base import S3ObjectRef, S3StorageClient
from core.ingestion import (
    BronzeDataset,
    BronzeParseResult,
    BronzeWrite,
    LandingDomain,
    LandingTarget,
)
from core.ingestion.landing import PartitionedLandingTarget
from core.ingestion.parser import (
    attach_source_uri,
    parse_best_effort_rows,
    parse_date,
    parse_decimal,
    parse_optional_decimal,
    parse_strict_rows,
)
from core.ingestion.partitioning import LandingPartitionSchema
from core.models import BronzeModel, ProviderModel
from core.schema import BronzeTableModel


class RawProviderItem(ProviderModel):
    """Provider payload with an alias to verify landing serialization."""

    provider: ClassVar[str] = "demo"
    provider_name: str = Field(alias="ProviderName")
    close_value: Decimal = Field(alias="Close")


class PriceRow(BronzeModel):
    """Minimal Bronze row for ingestion tests."""

    provider_exchange_code: str
    ticker: str
    bar_date: date
    close: Decimal


class OtherRow(BronzeModel):
    """A different Bronze row shape used to test dataset row validation."""

    name: str


class PriceTable(BronzeTableModel):
    """Minimal Bronze table metadata for ingestion tests."""

    table_name = "eod_price"
    row_model = PriceRow
    unique_columns = ("provider_exchange_code", "ticker", "bar_date", "data_provider")
    idempotency_columns = ("provider_exchange_code", "bar_date")


class DailyPricePartition(LandingPartitionSchema):
    """Daily price landing partitions."""

    provider_exchange_code: str
    bar_date: date


class BackfillPricePartition(LandingPartitionSchema):
    """Backfill price landing partitions."""

    provider_exchange_code: str
    ticker: str
    from_date: date
    to_date: date


@dataclass(frozen=True, slots=True)
class PriceLandings:
    """Typed landing group for daily and backfill price payloads."""

    daily: PartitionedLandingTarget[DailyPricePartition]
    backfill: PartitionedLandingTarget[BackfillPricePartition]


class FakeS3:
    """Minimal S3 fake for landing target tests."""

    saved_key: str | None = None
    saved_data: Any = None
    saved_format: str | None = None

    def save(
        self,
        key: str,
        data: Any,
        *,
        format: str | None = None,
        bucket: str | None = None,
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
        extra_args: Mapping[str, Any] | None = None,
    ) -> S3ObjectRef:
        """Capture the save request and return an S3 object ref."""
        self.saved_key = key
        self.saved_data = data
        self.saved_format = format
        return S3ObjectRef(bucket=bucket or "landing-bucket", key=key)


class FakeLake:
    """Minimal lake fake for Bronze write and idempotency tests."""

    query_result: dict[str, Any] | None
    last_query: str | None
    last_params: Sequence[Any] | None
    inserted_schema: str | None
    inserted_table: str | None
    inserted_rows: list[dict[str, Any]]

    def __init__(self, query_result: dict[str, Any] | None = None) -> None:
        """Create the fake with an optional query result."""
        self.query_result = query_result
        self.last_query = None
        self.last_params = None
        self.inserted_schema = None
        self.inserted_table = None
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


DAILY_LANDING = LandingTarget.partitioned(
    LandingDomain.EOD_PRICE,
    partition_fields=DailyPricePartition,
)
BACKFILL_LANDING = LandingTarget.partitioned(
    LandingDomain.EOD_PRICE,
    partition_fields=BackfillPricePartition,
)
PRICE_DATASET = BronzeDataset(
    provider="eodhd",
    table=PriceTable,
    landings=PriceLandings(daily=DAILY_LANDING, backfill=BACKFILL_LANDING),
)


def _price(close: str = "190.75") -> PriceRow:
    return PriceRow(
        provider_exchange_code="US",
        ticker="AAPL.US",
        bar_date=date(2026, 5, 9),
        close=Decimal(close),
    )


def test_snapshot_landing_key_matches_canonical_path() -> None:
    """Snapshot keys keep the old canonical landing path shape."""
    target = LandingTarget.snapshot(LandingDomain.EXCHANGE)

    key = target.key(
        provider="eodhd",
        snapshot_date=date(2026, 5, 23),
        ingested_at=date(2026, 5, 24),
    )

    assert key == "landing/eodhd/exchange/snapshot_date=2026-05-23/ingested_at=2026-05-24/exchange.jsonl"


def test_partitioned_landing_key_uses_declared_order_and_ingested_at() -> None:
    """Partitioned keys use declared partition order plus path-safe ingested_at."""
    key = PRICE_DATASET.landings.daily.key(
        provider=PRICE_DATASET.provider,
        partitions=DailyPricePartition(provider_exchange_code="US", bar_date=date(2026, 5, 24)),
        ingested_at=datetime(2026, 5, 24, 12, 30),
    )

    assert key == (
        "landing/eodhd/eod_price/provider_exchange_code=US/"
        "bar_date=2026-05-24/ingested_at=2026-05-24T12-30-00Z/data.jsonl"
    )


def test_partitioned_landing_key_requires_declared_fields() -> None:
    """Missing partition values fail before building a storage key."""
    with pytest.raises(ValueError, match="bar_date"):
        PRICE_DATASET.landings.daily.key(
            provider="eodhd",
            partitions=cast(DailyPricePartition, {"provider_exchange_code": "US"}),
            ingested_at=date(2026, 5, 24),
        )


def test_landing_target_save_uses_key_format_and_provider_aliases() -> None:
    """Landing saves pass an explicit format and preserve provider aliases."""
    s3 = FakeS3()
    payload = RawProviderItem(ProviderName="EODHD", Close=Decimal("190.75"))

    ref = PRICE_DATASET.landings.daily.save(
        cast(S3StorageClient, s3),
        provider=PRICE_DATASET.provider,
        data=payload,
        partitions=DailyPricePartition(provider_exchange_code="US", bar_date=date(2026, 5, 24)),
        ingested_at=date(2026, 5, 24),
    )

    expected_key = (
        "landing/eodhd/eod_price/provider_exchange_code=US/bar_date=2026-05-24/ingested_at=2026-05-24/data.jsonl"
    )
    assert ref.uri == f"s3://landing-bucket/{expected_key}"
    assert s3.saved_key == expected_key
    assert s3.saved_format == "jsonl"
    assert s3.saved_data == {"ProviderName": "EODHD", "Close": "190.75"}


def test_landing_target_builds_audit_metadata() -> None:
    """Bronze datasets infer landing-object audit datasets from landing field names."""
    ref = S3ObjectRef(
        bucket="landing-bucket",
        key="landing/eodhd/eod_price/provider_exchange_code=US/bar_date=2026-05-24/data.jsonl",
    )

    landing = PRICE_DATASET.landings.daily.landing_write(
        ref,
        partitions=DailyPricePartition(provider_exchange_code="US", bar_date=date(2026, 5, 24)),
        rows_raw=42,
    )

    assert landing.dataset == "eod_price.daily"
    assert landing.source_uri == ref.uri
    assert landing.partition == {
        "provider_exchange_code": "US",
        "bar_date": date(2026, 5, 24),
    }
    assert landing.rows_raw == 42


def test_explicit_landing_audit_dataset_overrides_inference() -> None:
    """Landing targets can still override the inferred audit dataset label."""
    explicit = LandingTarget.partitioned(
        LandingDomain.EOD_PRICE,
        partition_fields=DailyPricePartition,
        audit_dataset="custom.daily",
    )
    dataset = BronzeDataset(
        provider="eodhd",
        table=PriceTable,
        landings=PriceLandings(daily=explicit, backfill=BACKFILL_LANDING),
    )

    assert dataset.landings.daily.audit_dataset_name == "custom.daily"
    assert dataset.landings.backfill.audit_dataset_name == "eod_price.backfill"


def test_bronze_dataset_uses_typed_landing_group() -> None:
    """Multiple landing routes remain explicit and dot-accessible on the dataset."""
    assert PRICE_DATASET.landings.daily.partition_field_names == ("provider_exchange_code", "bar_date")
    assert PRICE_DATASET.landings.backfill.partition_field_names == (
        "provider_exchange_code",
        "ticker",
        "from_date",
        "to_date",
    )


def test_bronze_record_uses_raw_json_hash_and_source_ref() -> None:
    """Bronze serialization adds the standard ingestion envelope."""
    source = BronzeParseResult(row=_price(), raw_fragment={"code": "AAPL"})
    source_ref = S3ObjectRef(bucket="landing-bucket", key="landing/eodhd/eod_price/data.jsonl")

    record = PRICE_DATASET.bronze_record(source, source_ref=source_ref)
    same_row_other_raw = PRICE_DATASET.bronze_record(
        BronzeParseResult(row=source.row, raw_fragment={"code": "MSFT"}),
        source_ref=source_ref,
    )
    changed_row = PRICE_DATASET.bronze_record(
        BronzeParseResult(row=_price("191.50"), raw_fragment={"code": "AAPL"}),
        source_ref=source_ref,
    )

    assert json.loads(record["raw_json"]) == {"code": "AAPL"}
    assert record["row_hash"] == same_row_other_raw["row_hash"]
    assert record["row_hash"] != changed_row["row_hash"]
    assert record["source_uri"] == source_ref.uri
    assert record["data_provider"] == "eodhd"


def test_bronze_dataset_rejects_source_for_wrong_row_model() -> None:
    """The table-owned row model guards Bronze writes at runtime."""
    source = BronzeParseResult(row=OtherRow(name="wrong"), raw_fragment={})

    with pytest.raises(TypeError, match="eod_price expects BronzeParseResult row PriceRow"):
        PRICE_DATASET.bronze_record(source)


def test_attach_source_uri_accepts_s3_object_ref() -> None:
    """Landing object refs can be attached to parser output for lineage."""
    source_ref = S3ObjectRef(bucket="landing-bucket", key="landing/eodhd/eod_price/data.jsonl")
    sources = attach_source_uri([BronzeParseResult(row=_price(), raw_fragment={})], source_ref)

    assert sources[0].source_uri == source_ref.uri


def test_parse_strict_rows_wraps_raw_fragments() -> None:
    """Strict row parsing wraps every raw fragment for lineage."""
    raws: list[dict[str, str]] = [{"close": "190.75"}]

    sources = parse_strict_rows(raws, lambda raw: _price(raw["close"]))

    assert sources[0].row.close == Decimal("190.75")
    assert sources[0].raw_fragment is raws[0]


def test_parse_strict_rows_raises_on_bad_row() -> None:
    """Strict row parsing fails the whole batch on the first bad row."""

    def build_row(raw: str) -> PriceRow:
        raise ValueError(f"{raw} row")

    with pytest.raises(ValueError, match="bad row"):
        parse_strict_rows(["bad"], build_row)


def test_parse_best_effort_rows_tracks_rejections_and_drops() -> None:
    """Best-effort parsing separates valid rows, rejected rows, and intentional drops."""
    errors: list[tuple[str, str]] = []

    def build_row(raw: str) -> PriceRow | None:
        if raw == "drop":
            return None
        if raw == "bad":
            raise ValueError("bad row")
        return _price()

    sources, rejected = parse_best_effort_rows(
        ["ok", "drop", "bad"],
        build_row,
        on_rejected=lambda raw, exc: errors.append((raw, str(exc))),
    )

    assert [source.raw_fragment for source in sources] == ["ok"]
    assert rejected == ["bad"]
    assert errors == [("bad", "bad row")]


def test_parser_scalar_helpers_handle_provider_values() -> None:
    """Shared parser coercions cover common provider scalar shapes."""
    assert parse_date("2026-05-24") == date(2026, 5, 24)
    assert parse_decimal(190.75) == Decimal("190.75")
    assert parse_optional_decimal("") is None
    assert parse_optional_decimal("not-a-number") is None

    with pytest.raises(ValueError, match="Expected a numeric value"):
        parse_decimal(None)


def test_write_bronze_inserts_serialized_rows() -> None:
    """Bronze writes serialize sources and delegate one insert to the lake."""
    lake = FakeLake()

    written = PRICE_DATASET.write_bronze(
        cast(DataLakeClient, lake),
        [BronzeParseResult(row=_price(), raw_fragment={"code": "AAPL"})],
        source_uri="s3://landing/eod.jsonl",
    )

    assert written == 1
    assert lake.inserted_schema == "bronze"
    assert lake.inserted_table == "eod_price"
    assert lake.inserted_rows[0]["ticker"] == "AAPL.US"
    assert lake.inserted_rows[0]["source_uri"] == "s3://landing/eod.jsonl"


def test_write_bronze_skips_empty_sources() -> None:
    """Empty Bronze writes do not call into the lake client."""
    lake = FakeLake()

    assert PRICE_DATASET.write_bronze(cast(DataLakeClient, lake), []) == 0
    assert lake.inserted_rows == []


def test_bronze_write_result_marks_intentional_skips() -> None:
    """Domain write tasks can return a reason when no Bronze rows were written."""
    assert BronzeWrite(rows_written=0, reason="already_ingested").skipped
    assert not BronzeWrite(rows_written=0).skipped
    assert not BronzeWrite(rows_written=3).skipped


def test_already_ingested_checks_idempotency_partition_and_provider() -> None:
    """Idempotency checks include declared table keys plus data_provider."""
    lake = FakeLake(query_result={"cnt": 1})

    exists = PRICE_DATASET.already_ingested(
        cast(DataLakeClient, lake),
        provider_exchange_code="US",
        bar_date=date(2026, 5, 9),
    )

    assert exists is True
    assert lake.last_query == (
        "SELECT COUNT(*) AS cnt FROM bronze.eod_price "
        "WHERE provider_exchange_code = ? AND bar_date = ? AND data_provider = ?"
    )
    assert lake.last_params == ["US", "2026-05-09", "eodhd"]
