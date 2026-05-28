"""Tests for dbt-built exchange provider universe readers."""

from collections.abc import Sequence
from typing import Any

from pytest import MonkeyPatch

from domains.exchange import provider_universe


class FakeLake:
    """Minimal lake fake for provider universe reader tests."""

    def __init__(self, *, exists: bool, rows: list[dict[str, Any]] | None = None) -> None:
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


def test_load_provider_exchange_codes_falls_back_when_contract_missing(monkeypatch: MonkeyPatch) -> None:
    """Fresh environments can still run a narrow default scope before dbt builds."""
    lake = FakeLake(exists=False)
    monkeypatch.setattr(provider_universe, "get_lake_client", lambda: lake)

    assert provider_universe.load_provider_exchange_codes("eodhd", fallback=("US",)) == ["US"]
