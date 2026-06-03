"""Tests for fundamentals flow orchestration branches."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import date
from typing import cast

import httpx
import pytest

from core.clients.http.base import ProviderRateLimitError
from core.ingestion import BronzeWrite, LandingWrite, RunUnitTally
from core.ingestion.run_tracking import UnitStatus
from domains.fundamental import flows, tasks
from domains.fundamental.parsers import parse_fundamental_document
from providers.eodhd.models import FundamentalRaw

SNAPSHOT_DATE = date(2026, 5, 29)


def _provider_rate_limit_error(symbol: str) -> ProviderRateLimitError:
    """Build a redacted provider rate-limit error for flow branch tests."""
    request = httpx.Request("GET", f"https://provider.example/eod/{symbol}?api_token=[redacted]")
    response = httpx.Response(429, request=request)
    return ProviderRateLimitError("provider quota exhausted", request=request, response=response)


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

    def record_unit_with_landing(self, landing: LandingWrite, **kwargs: object) -> str:
        """Capture a completed unit row associated with a landing object."""
        self.units.append({"source_uri": landing.source_uri, **kwargs})
        status = kwargs.get("status")
        if isinstance(status, str):
            self.tally.record(cast(UnitStatus, status))
        return "unit-1"

    def record_rejections(self, _: object) -> None:
        """Accept parser rejection rows."""

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
async def test_continue_ingestion_batch_uses_resolved_snapshot_date(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backfill continuation keeps one bronze partition across multi-day reruns."""
    run = FakeRun()
    resolve_calls: dict[str, object] = {}

    def fake_resolve(**kwargs: object) -> dict[str, str]:
        resolve_calls.update(kwargs)
        return {"snapshot_date": "2026-05-15", "source": "bronze_latest"}

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(tasks, "resolve_fundamental_snapshot_date_task", fake_resolve)
    monkeypatch.setattr(flows, "fundamental_document_already_ingested", lambda *_: True)

    summary = await flows.fundamental_flow.fn(
        tickers=["AAPL.US"],
        continue_ingestion_batch=True,
    )

    assert resolve_calls["continue_ingestion_batch"] is True
    assert summary["snapshot_date"] == "2026-05-15"
    assert summary["snapshot_date_source"] == "bronze_latest"


@pytest.mark.asyncio
async def test_fundamental_flow_uses_provider_universe_for_default_tickers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Automatic fundamentals selection should use fundamentals-approved provider codes."""
    run = FakeRun()
    loaded_codes: list[list[str]] = []

    def fetch_codes() -> list[str]:
        return ["US"]

    def load_tickers(provider_exchange_codes: list[str] | None, limit: int | None = None) -> list[str]:
        loaded_codes.append(list(provider_exchange_codes or []))
        assert limit == 5
        return []

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "fetch_fundamental_provider_exchange_codes", fetch_codes)
    monkeypatch.setattr(flows, "load_fundamental_stock_tickers", load_tickers)

    summary = await flows.fundamental_flow.fn(snapshot_date=SNAPSHOT_DATE, limit=5)

    assert loaded_codes == [["US"]]
    assert summary["tickers"] == {}
    assert run.completed_summary == summary


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
    monkeypatch.setattr(flows, "write_bronze_fundamental_stock_insider_transactions", fail_if_called)
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


@pytest.mark.asyncio
async def test_replay_landing_uses_landed_json_without_provider_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    """replay_landing=True reads existing landing JSON and reuses that source URI for Bronze writes."""
    raw = _raw_stock_payload()
    run = FakeRun()
    calls: list[str] = []
    source_uri = "s3://lake/landing/fundamental/provider=eodhd/provider_exchange_code=US/ticker=AAPL.US/data.json"

    async def load_landing(_: str, __: date, *, source_uri: str | None = None) -> tuple[FundamentalRaw, LandingWrite]:
        calls.append(f"load:{source_uri}")
        return raw, LandingWrite(
            dataset="fundamental.document",
            source_uri=source_uri or "s3://lake/latest.json",
            partition={"ticker": "AAPL.US", "snapshot_date": SNAPSHOT_DATE},
            rows_raw=1,
        )

    async def fail_fetch(_: str) -> FundamentalRaw:
        raise AssertionError("replay should not fetch from provider")

    async def fail_landing(*_: object, **__: object) -> None:
        raise AssertionError("replay should not write a new landing object")

    def write_one(*_: object, **__: object) -> BronzeWrite:
        return BronzeWrite(rows_written=1)

    def write_none(*_: object, **__: object) -> BronzeWrite:
        return BronzeWrite(rows_written=0, reason="empty")

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "fundamental_document_already_ingested", lambda *_: False)
    monkeypatch.setattr(flows, "fetch_fundamental_ticker", fail_fetch)
    monkeypatch.setattr(flows, "load_fundamental_from_landing", load_landing)
    monkeypatch.setattr(flows, "parse_fundamental_stock", tasks.parse_fundamental_stock.fn)
    monkeypatch.setattr(flows, "load_fundamental_document_payload_hash", lambda *_: None)
    monkeypatch.setattr(flows, "write_fundamental_to_landing", fail_landing)
    monkeypatch.setattr(flows, "write_bronze_fundamental_document", write_one)
    monkeypatch.setattr(flows, "write_bronze_fundamental_stock_identity", write_one)
    monkeypatch.setattr(flows, "write_bronze_fundamental_statement_facts", write_one)
    for name in (
        "write_bronze_fundamental_stock_earnings_facts",
        "write_bronze_fundamental_stock_shares_stats",
        "write_bronze_fundamental_stock_outstanding_shares",
        "write_bronze_fundamental_stock_holders",
        "write_bronze_fundamental_stock_insider_transactions",
        "write_bronze_fundamental_stock_splits_dividends",
        "write_bronze_fundamental_stock_dividend_counts",
        "write_bronze_fundamental_stock_metric_facts",
        "write_bronze_fundamental_stock_esg_activities",
        "write_bronze_fundamental_etf_identity",
        "write_bronze_fundamental_mutual_fund_identity",
        "write_bronze_fundamental_index_identity",
        "write_bronze_fundamental_etf_holdings",
        "write_bronze_fundamental_mutual_fund_holdings",
        "write_bronze_fundamental_fund_metric_facts",
        "write_bronze_fundamental_index_components",
        "write_bronze_fundamental_index_historical_components",
    ):
        monkeypatch.setattr(flows, name, write_none)

    summary = await flows.fundamental_flow.fn(
        tickers=["AAPL.US"],
        snapshot_date=SNAPSHOT_DATE,
        replay_landing=True,
        landing_source_uris_by_ticker={"AAPL.US": source_uri},
    )

    assert calls == [f"load:{source_uri}"]
    ticker_summary = cast(dict[str, object], cast(dict[str, object], summary["tickers"])["AAPL.US"])
    assert ticker_summary["rows_written"] == 3
    assert run.units[0]["source_uri"] == source_uri
    assert run.is_terminal is True


@pytest.mark.asyncio
async def test_provider_credit_budget_skips_tickers_before_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    """max_provider_credits caps paid fundamentals calls before fetch tasks are submitted."""
    raw = _raw_stock_payload()
    payload_hash = parse_fundamental_document(raw, ticker="AAPL.US", snapshot_date=SNAPSHOT_DATE).row.payload_hash
    run = FakeRun()
    calls: list[str] = []

    async def fetch(ticker: str) -> FundamentalRaw:
        calls.append(ticker)
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

    summary = await flows.fundamental_flow.fn(
        tickers=["AAPL.US", "MSFT.US"],
        snapshot_date=SNAPSHOT_DATE,
        skip_existing=False,
        max_provider_credits=10,
        provider_credits_per_call=10,
    )

    assert calls == ["AAPL.US"]
    assert summary["skipped"] == ["MSFT.US", "AAPL.US"]
    assert [unit["reason"] for unit in run.units] == ["credit_budget_exhausted", "payload_unchanged"]


@pytest.mark.asyncio
async def test_fundamental_flow_defers_remaining_tickers_after_provider_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 429 stops later fetch batches so the next run can resume from Bronze idempotency."""
    raw = _raw_stock_payload()
    payload_hash = parse_fundamental_document(raw, ticker="AAPL.US", snapshot_date=SNAPSHOT_DATE).row.payload_hash
    run = FakeRun()
    calls: list[str] = []

    async def fetch(ticker: str) -> FundamentalRaw:
        calls.append(ticker)
        if ticker == "MSFT.US":
            raise _provider_rate_limit_error(ticker)
        return raw

    def fail_if_called(*_: object, **__: object) -> None:
        raise AssertionError("unchanged or deferred tickers should not land, delete, or write")

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "fundamental_document_already_ingested", lambda *_: False)
    monkeypatch.setattr(flows, "fetch_fundamental_ticker", fetch)
    monkeypatch.setattr(flows, "parse_fundamental_stock", tasks.parse_fundamental_stock.fn)
    monkeypatch.setattr(flows, "load_fundamental_document_payload_hash", lambda *_: payload_hash)
    monkeypatch.setattr(flows, "write_fundamental_to_landing", fail_if_called)
    monkeypatch.setattr(flows, "delete_fundamental_snapshot_rows", fail_if_called)
    monkeypatch.setattr(flows, "write_bronze_fundamental_document", fail_if_called)

    summary = await flows.fundamental_flow.fn(
        tickers=["AAPL.US", "MSFT.US", "GOOG.US"],
        snapshot_date=SNAPSHOT_DATE,
        skip_existing=False,
        batch_size=1,
    )

    assert calls == ["AAPL.US", "MSFT.US"]
    assert summary["provider_quota_exhausted"] is True
    assert summary["failed"] == ["MSFT.US"]
    assert summary["deferred"] == ["MSFT.US", "GOOG.US"]
    assert summary["skipped"] == ["AAPL.US", "GOOG.US"]
    assert [unit["reason"] for unit in run.units] == [
        "payload_unchanged",
        "provider_rate_limited",
        "provider_rate_limited",
    ]


def test_delete_fundamental_snapshot_rows_deletes_child_tables_before_document(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refresh replacement deletes statement and identity rows before document metadata."""
    lake = FakeLake()
    import core.clients.lake as lake_module

    monkeypatch.setattr(lake_module, "get_lake_client", lambda: lake)

    deleted_tables = tasks.delete_fundamental_snapshot_rows.fn("AAPL.US", SNAPSHOT_DATE)

    assert deleted_tables == 20
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
        "bronze.fundamental_stock_insider_transaction",
        "bronze.fundamental_stock_holder",
        "bronze.fundamental_stock_outstanding_shares",
        "bronze.fundamental_stock_shares_stats",
        "bronze.fundamental_stock_earnings_fact",
        "bronze.fundamental_statement_fact",
        "bronze.fundamental_stock_identity",
        "bronze.fundamental_document",
    ]
    assert all(params == [SNAPSHOT_DATE.isoformat(), "AAPL.US", "eodhd"] for _, params in lake.executed)
