"""Tests for EOD price flow orchestration branches."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import date
from typing import cast

import httpx
import pytest

from core.clients.http.base import ProviderRateLimitError
from core.ingestion import BronzeWrite, LandingWrite, RunUnitTally
from core.ingestion.run_tracking import UnitStatus
from domains.eod_price import flows
from providers.eodhd.models import EODPriceBarRaw

FROM_DATE = date(2026, 5, 1)
TO_DATE = date(2026, 5, 31)


class FakeRun:
    """Minimal run scope fake for EOD backfill branch tests."""

    def __init__(self) -> None:
        """Create an empty fake run."""
        self.run_id = "run-1"
        self.tally = RunUnitTally()
        self.is_terminal = False
        self.units: list[dict[str, object]] = []
        self.completed_summary: dict[str, object] | None = None

    def record_unit(self, **kwargs: object) -> str:
        """Capture one unit row and update tally."""
        self.units.append(kwargs)
        status = kwargs.get("status")
        if isinstance(status, str):
            self.tally.record(cast(UnitStatus, status))
        return str(kwargs.get("unit_id") or "unit-1")

    def unit_record(self, **kwargs: object) -> dict[str, object]:
        """Build a unit row for later batched recording."""
        return kwargs

    def landing_object_record(self, landing: LandingWrite, *, unit_id: str) -> dict[str, object]:
        """Build a landing record for later batched recording."""
        return {"unit_id": unit_id, "source_uri": landing.source_uri}

    def record_units(self, records: Sequence[dict[str, object]]) -> list[str]:
        """Capture batched unit rows and update tally."""
        unit_ids: list[str] = []
        for record in records:
            self.units.append(record)
            unit_ids.append(str(record.get("unit_id") or "unit-1"))
            status = record.get("status")
            if isinstance(status, str):
                self.tally.record(cast(UnitStatus, status))
        return unit_ids

    def record_landing_objects(self, _: object) -> None:
        """Accept landing rows."""

    def record_rejections(self, _: object) -> None:
        """Accept parser rejection rows."""

    def rejection_record(self, **kwargs: object) -> dict[str, object]:
        """Build a rejection row for later batched recording."""
        return kwargs

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


def _provider_rate_limit_error(symbol: str) -> ProviderRateLimitError:
    """Build a redacted provider rate-limit error for flow branch tests."""
    request = httpx.Request("GET", f"https://provider.example/eod/{symbol}?api_token=[redacted]")
    response = httpx.Response(429, request=request)
    return ProviderRateLimitError("provider quota exhausted", request=request, response=response)


async def _write_empty_landing(
    raw_bars: list[EODPriceBarRaw],
    symbol: str,
    provider_exchange_code: str,
    from_date: date,
    to_date: date,
) -> LandingWrite:
    """Return a fake landing record for a fetched symbol."""
    return LandingWrite(
        dataset="eod_price.backfill",
        source_uri=f"s3://bucket/eod_price/{symbol}.jsonl",
        partition={
            "provider_exchange_code": provider_exchange_code,
            "ticker": symbol,
            "from_date": from_date,
            "to_date": to_date,
        },
        rows_raw=len(raw_bars),
    )


def _rejected_bar() -> EODPriceBarRaw:
    """Build a provider row that fails OHLC validation."""
    return EODPriceBarRaw.model_validate(
        {
            "date": "2026-05-09",
            "open": 100.0,
            "high": 90.0,
            "low": 80.0,
            "close": 85.0,
            "volume": 100,
            "adjusted_close": 85.0,
        }
    )


@pytest.mark.asyncio
async def test_eod_backfill_provider_call_budget_limits_submitted_symbols(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """max_provider_calls caps scheduled per-symbol fetches so a later run can resume from Bronze."""
    run = FakeRun()
    calls: list[str] = []

    async def fetch(symbol: str, _: date, __: date) -> list[EODPriceBarRaw]:
        calls.append(symbol)
        return []

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "load_backfill_pending_symbols", lambda *_: ["AAPL.US", "MSFT.US", "GOOG.US"])
    monkeypatch.setattr(flows, "fetch_ticker_eod_history", fetch)
    monkeypatch.setattr(flows, "write_ticker_eod_history_to_landing", _write_empty_landing)
    coverage_calls: list[str] = []

    def record_coverage(**kwargs: object) -> BronzeWrite:
        coverage_calls.append(str(kwargs["ticker"]))
        return BronzeWrite(rows_written=1)

    monkeypatch.setattr(flows, "write_eod_backfill_coverage", record_coverage)

    summary = await flows.eod_price_backfill_flow.fn(
        from_date=FROM_DATE,
        to_date=TO_DATE,
        provider_exchange_codes=["US"],
        batch_size=50,
        max_provider_calls=1,
    )

    assert coverage_calls == ["AAPL.US"]

    assert calls == ["AAPL.US"]
    assert summary["provider_quota_exhausted"] is True
    assert summary["provider_calls"] == {"max": 1, "submitted": 1, "deferred": 2}
    exchange = cast(dict[str, dict[str, object]], summary["exchange"])
    assert exchange["US"]["symbols_pending"] == 3
    assert exchange["US"]["symbols_deferred"] == 2
    assert run.completed_summary == summary


@pytest.mark.asyncio
async def test_eod_backfill_stops_after_provider_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 429 stops later backfill batches instead of failing every remaining symbol."""
    run = FakeRun()
    calls: list[str] = []

    async def fetch(symbol: str, _: date, __: date) -> list[EODPriceBarRaw]:
        calls.append(symbol)
        if symbol == "MSFT.US":
            raise _provider_rate_limit_error(symbol)
        return []

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "load_backfill_pending_symbols", lambda *_: ["AAPL.US", "MSFT.US", "GOOG.US"])
    monkeypatch.setattr(flows, "fetch_ticker_eod_history", fetch)
    monkeypatch.setattr(flows, "write_ticker_eod_history_to_landing", _write_empty_landing)
    monkeypatch.setattr(
        flows,
        "write_eod_backfill_coverage",
        lambda **_: BronzeWrite(rows_written=1),
    )

    summary = await flows.eod_price_backfill_flow.fn(
        from_date=FROM_DATE,
        to_date=TO_DATE,
        provider_exchange_codes=["US"],
        batch_size=1,
    )

    assert calls == ["AAPL.US", "MSFT.US"]
    assert summary["provider_quota_exhausted"] is True
    assert summary["failed_symbols"] == ["MSFT.US"]
    exchange = cast(dict[str, dict[str, object]], summary["exchange"])
    assert exchange["US"]["symbols_deferred"] == 1
    assert [unit.get("reason") for unit in run.units] == [
        "no_valid_rows",
        "provider_rate_limited",
        "symbol_failures",
    ]


@pytest.mark.asyncio
async def test_eod_backfill_all_rejected_rows_do_not_write_no_data_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parser failures should remain retryable instead of being marked terminal no_data."""
    run = FakeRun()

    async def fetch(_: str, __: date, ___: date) -> list[EODPriceBarRaw]:
        return [_rejected_bar()]

    def fail_coverage(**_: object) -> BronzeWrite:
        raise AssertionError("all rejected rows should not be recorded as no_data coverage")

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "load_backfill_pending_symbols", lambda *_: ["AAPL.US"])
    monkeypatch.setattr(flows, "fetch_ticker_eod_history", fetch)
    monkeypatch.setattr(flows, "write_ticker_eod_history_to_landing", _write_empty_landing)
    monkeypatch.setattr(flows, "write_eod_backfill_coverage", fail_coverage)

    summary = await flows.eod_price_backfill_flow.fn(
        from_date=FROM_DATE,
        to_date=TO_DATE,
        provider_exchange_codes=["US"],
        batch_size=1,
    )

    assert summary["provider_quota_exhausted"] is False
    assert [unit.get("reason") for unit in run.units] == ["all_rows_rejected", None]
