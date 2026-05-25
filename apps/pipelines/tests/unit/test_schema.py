"""Tests for the lightweight lake schema DSL."""

from datetime import date
from decimal import Decimal
from typing import Annotated, ClassVar

import pytest
from pydantic import BaseModel

from core.schema import DECIMAL, BronzeTableModel, SqlColumn


class ExchangeRow(BaseModel):
    """Small row model used to test inferred Bronze DDL."""

    snapshot_date: date
    provider_exchange_code: str
    name: str
    operating_mic_codes: str | None = None


class ExchangeTable(BronzeTableModel):
    """Small table used to test inferred Bronze DDL."""

    table_name = "exchange"
    row_model = ExchangeRow
    unique_columns = ("snapshot_date", "provider_exchange_code", "data_provider")
    idempotency_columns = ("snapshot_date",)


def test_bronze_table_model_generates_ddl_with_envelope() -> None:
    """Bronze DDL combines Pydantic fields and the standard ingestion envelope."""
    assert (
        ExchangeTable.to_ddl()
        == """CREATE TABLE IF NOT EXISTS bronze.exchange (
    ingestion_id UUID DEFAULT GEN_RANDOM_UUID(),
    snapshot_date DATE NOT NULL,
    provider_exchange_code VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    operating_mic_codes VARCHAR,
    data_provider VARCHAR NOT NULL,
    raw_json JSON NOT NULL,
    row_hash VARCHAR NOT NULL,
    source_uri VARCHAR,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (snapshot_date, provider_exchange_code, data_provider)
);"""
    )


def test_table_model_uses_separate_pydantic_row_model() -> None:
    """Table metadata stays separate from Pydantic row validation."""
    row = ExchangeRow(
        snapshot_date=date(2026, 5, 24),
        provider_exchange_code="US",
        name="USA Stocks",
    )
    assert row.model_dump(mode="json") == {
        "snapshot_date": "2026-05-24",
        "provider_exchange_code": "US",
        "name": "USA Stocks",
        "operating_mic_codes": None,
    }


def test_table_exposes_key_column_names() -> None:
    """Table metadata exposes validated unique and idempotency columns."""
    assert ExchangeTable.unique_column_names() == ("snapshot_date", "provider_exchange_code", "data_provider")
    assert ExchangeTable.idempotency_column_names() == ("snapshot_date",)


def test_annotated_sql_column_overrides_inferred_type() -> None:
    """Annotated SqlColumn metadata overrides default Python type inference."""

    class PriceRow(BaseModel):
        close: Annotated[Decimal, SqlColumn(DECIMAL, nullable=False)]

    class PriceTable(BronzeTableModel):
        table_name: ClassVar[str] = "price"
        row_model = PriceRow
        unique_columns = ()
        idempotency_columns = ()

    ddl = PriceTable.to_ddl()
    assert "close DECIMAL NOT NULL" in ddl


def test_unknown_unique_column_raises_at_definition_time() -> None:
    """Unique column references are checked when the table class is declared."""

    class BrokenRow(BaseModel):
        name: str

    with pytest.raises(ValueError, match="unique_columns.*missing_column"):

        class BrokenTable(BronzeTableModel):
            table_name: ClassVar[str] = "broken"
            row_model = BrokenRow
            unique_columns = ("missing_column",)
            idempotency_columns = ()


def test_unknown_idempotency_column_raises_at_definition_time() -> None:
    """Idempotency column references are checked when the table class is declared."""

    class BrokenRow(BaseModel):
        name: str

    with pytest.raises(ValueError, match="idempotency_columns.*missing_column"):

        class BrokenTable(BronzeTableModel):
            table_name: ClassVar[str] = "broken"
            row_model = BrokenRow
            unique_columns = ()
            idempotency_columns = ("missing_column",)


def test_required_column_metadata_raises_at_definition_time() -> None:
    """Unique and idempotency column metadata must be explicitly declared."""

    class Row(BaseModel):
        name: str

    with pytest.raises(TypeError, match="unique_columns"):

        class MissingUniqueColumnsTable(BronzeTableModel):
            table_name: ClassVar[str] = "missing_unique"
            row_model = Row
            idempotency_columns = ()

    with pytest.raises(TypeError, match="idempotency_columns"):

        class MissingIdempotencyColumnsTable(BronzeTableModel):
            table_name: ClassVar[str] = "missing_idempotency"
            row_model = Row
            unique_columns = ()
