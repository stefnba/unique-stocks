"""Tests for dbt-built exchange provider universe readers."""

from collections.abc import Sequence
from typing import Any

from pytest import MonkeyPatch

from domains.exchange import provider_universe


class FakeLake:
    """Minimal lake fake for provider universe reader tests."""

    def __init__(
        self,
        *,
        exists: bool,
        rows: list[dict[str, Any]] | None = None,
        columns: set[str] | None = None,
    ) -> None:
        """Create a fake lake with table existence and query rows."""
        self.exists = exists
        self.rows = rows or []
        self.columns = columns or {
            "is_enabled_for_ingestion",
            "is_enabled_for_instrument",
            "is_enabled_for_eod_price",
        }
        self.last_query: str | None = None
        self.last_params: Sequence[Any] | None = None
        self.last_query_one: str | None = None
        self.last_query_one_params: Sequence[Any] | None = None

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

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Capture and fake information_schema column checks."""
        self.last_query_one = sql
        self.last_query_one_params = params
        column = str((params or ["", "", ""])[2])
        return {"exists": 1} if column in self.columns else None


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
    assert lake.last_query_one_params == [
        "silver",
        "int_exchange_provider_ingestion_universe",
        "is_enabled_for_instrument",
    ]


def test_load_provider_exchange_codes_uses_aggregate_flag_for_old_contract(monkeypatch: MonkeyPatch) -> None:
    """Old Silver tables still work until exchange-build refreshes the contract."""
    lake = FakeLake(
        exists=True,
        rows=[{"provider_exchange_code": "US"}],
        columns={"is_enabled_for_ingestion"},
    )
    monkeypatch.setattr(provider_universe, "get_lake_client", lambda: lake)

    codes = provider_universe.load_provider_exchange_codes("eodhd", purpose="eod_price")

    assert codes == ["US"]
    assert lake.last_query is not None
    assert "is_enabled_for_ingestion" in lake.last_query
    assert "is_enabled_for_eod_price" not in lake.last_query


def test_load_provider_exchange_codes_falls_back_when_contract_missing(monkeypatch: MonkeyPatch) -> None:
    """Fresh environments can still run a narrow default scope before dbt builds."""
    lake = FakeLake(exists=False)
    monkeypatch.setattr(provider_universe, "get_lake_client", lambda: lake)

    assert provider_universe.load_provider_exchange_codes("eodhd", fallback=("US",)) == ["US"]
