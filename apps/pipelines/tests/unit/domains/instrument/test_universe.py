"""Tests for Silver ingestion contract helpers."""

import pytest

from domains.instrument.universe import (
    InstrumentUniverseContractError,
    SilverIngestionContractError,
    require_instrument_universe,
    require_silver_ingestion_model,
)


class FakeLake:
    """Minimal lake fake for Silver contract resolution."""

    def __init__(self, tables: set[tuple[str, str]]) -> None:
        """Configure table availability."""
        self.tables = tables

    def table_exists(self, schema: str, table: str) -> bool:
        """Return whether the fake exposes a table."""
        return (schema, table) in self.tables

    def qualified_name(self, schema: str, table: str) -> str:
        """Return a stable qualified table name."""
        return f"{schema}.{table}"


def test_require_instrument_universe_returns_silver_contract() -> None:
    """Instrument auto-selection should resolve the dbt-built Silver universe."""
    lake = FakeLake({("silver", "int_latest_instrument_universe")})

    assert require_instrument_universe(lake) == "silver.int_latest_instrument_universe"


def test_require_instrument_universe_raises_clear_contract_error() -> None:
    """Missing instrument universe should point operators at instrument-build."""
    lake = FakeLake(set())

    with pytest.raises(InstrumentUniverseContractError, match="instrument-build"):
        require_instrument_universe(lake)


def test_require_silver_ingestion_model_returns_qualified_name() -> None:
    """Generic ingestion-control contracts should resolve through Silver."""
    lake = FakeLake({("silver", "int_eod_price_backfill_symbol_status")})

    assert (
        require_silver_ingestion_model(
            lake,
            "int_eod_price_backfill_symbol_status",
            build_hint="dbt-build/price-build",
        )
        == "silver.int_eod_price_backfill_symbol_status"
    )


def test_require_silver_ingestion_model_raises_clear_contract_error() -> None:
    """Missing ingestion-control views should name the missing model and build."""
    lake = FakeLake(set())

    with pytest.raises(SilverIngestionContractError, match="dbt-build/price-build"):
        require_silver_ingestion_model(
            lake,
            "int_eod_price_backfill_symbol_status",
            build_hint="dbt-build/price-build",
        )
