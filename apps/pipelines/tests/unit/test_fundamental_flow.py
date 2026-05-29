"""Tests for fundamentals flow orchestration branches."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import date
from typing import cast

import pytest

from core.ingestion import RunUnitTally
from core.ingestion.run_tracking import UnitStatus
from domains.fundamental import flows, tasks
from domains.fundamental.parsers import parse_fundamental_document
from providers.eodhd.models import FundamentalRaw

SNAPSHOT_DATE = date(2026, 5, 29)


class FakeRun:
    """Minimal run scope fake for flow branch tests."""

    def __init__(self) -> None:
        """Create an empty fake run."""
        self.tally = RunUnitTally()
        self.is_terminal = False
        self.units: list[dict[str, object]] = []
        self.completed_summary: dict[str, object] | None = None

    def record_unit(self, **kwargs: object) -> str:
        """Capture a unit row and update tally."""
        self.units.append(kwargs)
        status = kwargs.get("status")
        if isinstance(status, str):
            self.tally.record(cast(UnitStatus, status))
        return "unit-1"

    def complete(self, *, summary: dict[str, object], **_: object) -> None:
        """Capture run completion."""
        self.completed_summary = summary
        self.is_terminal = True

    def fail(self, *_: object, **__: object) -> None:
        """Mark the run failed."""
        self.is_terminal = True


class FakeTracker:
    """Minimal tracker fake that returns one run context."""

    def __init__(self, run: FakeRun) -> None:
        """Create the tracker for a known fake run."""
        self.run = run

    @contextmanager
    def track_run(self, **_: object) -> Iterator[FakeRun]:
        """Yield the fake run."""
        yield self.run


class FakeLake:
    """Minimal lake fake for delete task tests."""

    def __init__(self) -> None:
        """Create an empty fake lake."""
        self.executed: list[tuple[str, Sequence[object] | None]] = []

    def table_exists(self, schema: str, table: str) -> bool:
        """Pretend all requested fundamentals tables exist."""
        return schema == "bronze" and table.startswith("fundamental_")

    def qualified_name(self, schema: str, table: str) -> str:
        """Return a stable unquoted table name for assertions."""
        return f"{schema}.{table}"

    def execute(self, sql: str, params: Sequence[object] | None = None) -> None:
        """Capture executed SQL."""
        self.executed.append((sql, params))


def _raw_stock_payload() -> FundamentalRaw:
    """Build a compact stock fundamentals document."""
    return FundamentalRaw.model_validate(
        {
            "General": {
                "Code": "AAPL",
                "Type": "Common Stock",
                "Name": "Apple Inc",
                "Exchange": "NASDAQ",
                "PrimaryTicker": "AAPL.US",
                "UpdatedAt": "2026-05-28",
            },
            "Financials": {
                "Balance_Sheet": {
                    "currency_symbol": "USD",
                    "quarterly": {
                        "2026-03-31": {
                            "date": "2026-03-31",
                            "currency_symbol": "USD",
                            "totalAssets": "371082000000.00",
                        }
                    },
                    "yearly": {},
                }
            },
        }
    )


@pytest.mark.asyncio
async def test_skip_existing_false_uses_changed_payload_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """skip_existing=False fetches and skips unchanged same-day payloads."""
    raw = _raw_stock_payload()
    payload_hash = parse_fundamental_document(raw, ticker="AAPL.US", snapshot_date=SNAPSHOT_DATE).row.payload_hash
    run = FakeRun()
    calls: list[str] = []

    async def fetch(_: str) -> FundamentalRaw:
        calls.append("fetch")
        return raw

    def fail_if_called(*_: object, **__: object) -> None:
        raise AssertionError("unchanged refresh should not land, delete, or write")

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "fetch_fundamental_ticker", fetch)
    monkeypatch.setattr(flows, "parse_fundamental_stock", tasks.parse_fundamental_stock.fn)
    monkeypatch.setattr(flows, "load_fundamental_document_payload_hash", lambda *_: payload_hash)
    monkeypatch.setattr(flows, "write_fundamental_to_landing", fail_if_called)
    monkeypatch.setattr(flows, "delete_fundamental_snapshot_rows", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_document", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_stock_identity", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_statement_facts", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_stock_earnings_facts", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_stock_shares_stats", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_stock_outstanding_shares", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_stock_holders", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_stock_splits_dividends", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_stock_dividend_counts", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_stock_metric_facts", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_stock_esg_activities", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_etf_identity", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_mutual_fund_identity", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_index_identity", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_etf_holdings", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_mutual_fund_holdings", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_fund_metric_facts", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_index_components", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_index_historical_components", fail_if_called)

    summary = await flows.fundamental_flow.fn(
        tickers=["AAPL.US"],
        snapshot_date=SNAPSHOT_DATE,
        skip_existing=False,
        refresh_existing=False,
    )

    assert calls == ["fetch"]
    assert summary["skipped"] == ["AAPL.US"]
    assert summary["tickers"] == {}
    assert run.units[0]["reason"] == "payload_unchanged"
    assert run.units[0]["rows_raw"] == 1
    assert run.is_terminal is True


def test_delete_fundamental_snapshot_rows_deletes_child_tables_before_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refresh replacement deletes statement and identity rows before document metadata."""
    lake = FakeLake()
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    deleted_tables = tasks.delete_fundamental_snapshot_rows.fn("AAPL.US", SNAPSHOT_DATE)

    assert deleted_tables == 19
    assert [sql.split("DELETE FROM ", maxsplit=1)[1].split()[0] for sql, _ in lake.executed] == [
        "bronze.fundamental_index_historical_component",
        "bronze.fundamental_index_component",
        "bronze.fundamental_fund_metric_fact",
        "bronze.fundamental_mutual_fund_holding",
        "bronze.fundamental_etf_holding",
        "bronze.fundamental_index_identity",
        "bronze.fundamental_mutual_fund_identity",
        "bronze.fundamental_etf_identity",
        "bronze.fundamental_stock_esg_activity",
        "bronze.fundamental_stock_metric_fact",
        "bronze.fundamental_stock_dividend_count",
        "bronze.fundamental_stock_splits_dividends",
        "bronze.fundamental_stock_holder",
        "bronze.fundamental_stock_outstanding_shares",
        "bronze.fundamental_stock_shares_stats",
        "bronze.fundamental_stock_earnings_fact",
        "bronze.fundamental_statement_fact",
        "bronze.fundamental_stock_identity",
        "bronze.fundamental_document",
    ]
    assert all(params == [SNAPSHOT_DATE.isoformat(), "AAPL.US", "eodhd"] for _, params in lake.executed)
