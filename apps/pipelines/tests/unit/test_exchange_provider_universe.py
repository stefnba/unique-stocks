"""Tests for dbt-built exchange provider universe readers."""

from collections.abc import Sequence
from typing import Any

import pytest
from pytest import MonkeyPatch

from domains.exchange import provider_universe


class FakeLake:
    """Minimal lake fake for provider universe reader tests."""

    def __init__(
        self,
        *,
        exists: bool,
        rows: list[dict[str, Any]] | None = None,
    ) -> None:
        """Create a fake lake with table existence and query rows."""
        self.exists = exists
        self.rows = rows or []
        self.last_query: str | None = None
        self.last_params: Sequence[Any] | None = None

    def table_exists(self, schema: str, table: str) -> bool:
        """Return configured table existence."""
        return self.exists and schema == "silver" and table == "int_exchange_provider_ingestion_universe"

    def qualified_name(self, schema: str, table: str) -> str:
        """Return a simple qualified name for query assertions."""
        return f"{schema}.{table}"

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Capture and return configured query rows."""
        self.last_query = sql
        self.last_params = params
        return self.rows


def test_load_provider_exchange_codes_reads_silver_contract(monkeypatch: MonkeyPatch) -> None:
    """Provider code readers should consume the dbt-built operational universe."""
    lake = FakeLake(
        exists=True,
        rows=[{"provider_exchange_code": "LSE"}, {"provider_exchange_code": "US"}],
    )
    monkeypatch.setattr(provider_universe, "get_lake_client", lambda: lake)

    codes = provider_universe.load_provider_exchange_codes("eodhd")

    assert codes == ["LSE", "US"]
    assert lake.last_params == ["eodhd"]
    assert lake.last_query is not None
    assert "silver.int_exchange_provider_ingestion_universe" in lake.last_query
    assert "is_enabled_for_ingestion" in lake.last_query


def test_load_provider_exchange_codes_filters_by_purpose(monkeypatch: MonkeyPatch) -> None:
    """Purpose-specific callers should use the matching Silver eligibility flag."""
    lake = FakeLake(
        exists=True,
        rows=[{"provider_exchange_code": "INDX"}, {"provider_exchange_code": "XETRA"}],
    )
    monkeypatch.setattr(provider_universe, "get_lake_client", lambda: lake)

    codes = provider_universe.load_provider_exchange_codes("eodhd", purpose="instrument")

    assert codes == ["INDX", "XETRA"]
    assert lake.last_query is not None
    assert "is_enabled_for_instrument" in lake.last_query


def test_load_provider_exchange_codes_filters_fundamental_purpose(monkeypatch: MonkeyPatch) -> None:
    """Fundamentals should have its own eligibility flag."""
    lake = FakeLake(exists=True, rows=[{"provider_exchange_code": "US"}])
    monkeypatch.setattr(provider_universe, "get_lake_client", lambda: lake)

    codes = provider_universe.load_provider_exchange_codes("eodhd", purpose="fundamental")

    assert codes == ["US"]
    assert lake.last_query is not None
    assert "is_enabled_for_fundamental" in lake.last_query


def test_load_provider_exchange_codes_uses_requested_purpose_flag_directly(monkeypatch: MonkeyPatch) -> None:
    """Purpose-specific callers should not bridge through old aggregate contract checks."""
    lake = FakeLake(exists=True, rows=[{"provider_exchange_code": "US"}])
    monkeypatch.setattr(provider_universe, "get_lake_client", lambda: lake)

    codes = provider_universe.load_provider_exchange_codes("eodhd", purpose="eod_price")

    assert codes == ["US"]
    assert lake.last_query is not None
    assert "is_enabled_for_eod_price" in lake.last_query
    assert "information_schema.columns" not in lake.last_query


def test_load_provider_exchange_codes_raises_when_contract_missing(monkeypatch: MonkeyPatch) -> None:
    """Fresh environments should fail until the provider universe contract is built."""
    lake = FakeLake(exists=False)
    monkeypatch.setattr(provider_universe, "get_lake_client", lambda: lake)

    with pytest.raises(provider_universe.ProviderUniverseContractError, match="run exchange-build"):
        provider_universe.load_provider_exchange_codes("eodhd")


def test_load_provider_exchange_codes_raises_when_no_codes_enabled(monkeypatch: MonkeyPatch) -> None:
    """Empty policy results should fail instead of silently changing scope."""
    lake = FakeLake(exists=True, rows=[])
    monkeypatch.setattr(provider_universe, "get_lake_client", lambda: lake)

    with pytest.raises(provider_universe.ProviderUniverseContractError, match="No provider exchange codes enabled"):
        provider_universe.load_provider_exchange_codes("eodhd", purpose="eod_price")
