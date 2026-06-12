"""Tests for EOD price flow orchestration branches."""

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import date
from typing import cast

import httpx
import pytest

from core.clients.http.base import ProviderRateLimitError
from core.ingestion import BronzeWrite, LandingWrite, RunUnitTally
from core.ingestion.run_tracking import UnitStatus
from domains.eod_price import flows
from providers.eodhd.models import EODBulkPriceRaw, EODPriceBarRaw

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
        self.completed_status: object | None = None
        self.completed_summary: dict[str, object] | None = None
        self.failed_error: object | None = None
        self.failed_summary: dict[str, object] | None = None

    def record_unit(self, **kwargs: object) -> str:
        """Capture one unit row and update tally."""
        self.units.append(kwargs)
        status = kwargs.get("status")
        if isinstance(status, str):
            self.tally.record(cast(UnitStatus, status))
        return str(kwargs.get("unit_id") or "unit-1")

    def record_unit_with_landing(self, _: LandingWrite, **kwargs: object) -> str:
        """Capture one unit row with an attached landing object."""
        return self.record_unit(**kwargs)

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

    def complete(self, *, summary: dict[str, object], **kwargs: object) -> None:
        """Capture run completion."""
        self.completed_status = kwargs.get("status")
        self.completed_summary = summary
        self.is_terminal = True

    def fail(self, *args: object, **kwargs: object) -> None:
        """Mark the run failed."""
        self.failed_error = args[0] if args else None
        self.failed_summary = cast(dict[str, object] | None, kwargs.get("summary"))
        self.is_terminal = True


class FakeTracker:
    """Minimal tracker fake that returns one run context."""

    def __init__(self, run: FakeRun) -> None:
        """Create the tracker for a known fake run."""
        self.run = run

    @contextmanager
    def track_run(self, **_: object) -> Generator[FakeRun]:
        """Yield the fake run."""
        yield self.run


def _provider_rate_limit_error(symbol: str) -> ProviderRateLimitError:
    """Build a redacted provider rate-limit error for flow branch tests."""
    request = httpx.Request("GET", f"https://provider.example/eod/{symbol}?api_token=[redacted]")
    response = httpx.Response(429, request=request)
    return ProviderRateLimitError("provider quota exhausted", request=request, response=response)


async def _write_empty_landing(
    raw_bars: list[EODPriceBarRaw],
    provider_exchange_code: str,
    provider_instrument_code: str,
    from_date: date | None,
    to_date: date,
) -> LandingWrite:
    """Return a fake landing record for a fetched instrument."""
    return LandingWrite(
        dataset="eod_price.backfill",
        source_uri=f"s3://bucket/eod_price/{provider_exchange_code}/{provider_instrument_code}.jsonl",
        partition={
            "provider_exchange_code": provider_exchange_code,
            "provider_instrument_code": provider_instrument_code,
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


def _valid_bar() -> EODPriceBarRaw:
    """Build a provider row that passes OHLC validation."""
    return EODPriceBarRaw.model_validate(
        {
            "date": "2026-05-09",
            "open": 100.0,
            "high": 110.0,
            "low": 90.0,
            "close": 105.0,
            "volume": 100,
            "adjusted_close": 105.0,
        }
    )


def _bulk_row(*, code: str = "AAPL", row_date: date = TO_DATE) -> EODBulkPriceRaw:
    """Build a provider bulk row for daily flow tests."""
    return EODBulkPriceRaw.model_validate(
        {
            "code": code,
            "date": row_date.isoformat(),
            "open": 100.0,
            "high": 110.0,
            "low": 90.0,
            "close": 105.0,
            "volume": 100,
            "adjusted_close": 105.0,
        }
    )


@pytest.mark.asyncio
async def test_eod_daily_explicit_trade_date_skips_already_ingested_exchange(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit daily reruns should skip provider fetch when Bronze already has the exchange/date."""
    run = FakeRun()

    async def fail_fetch(**_: object) -> list[object]:
        raise AssertionError("already ingested explicit trade_date should not fetch")

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "eod_price_already_ingested", lambda *_: True)
    monkeypatch.setattr(flows, "fetch_eod_price_bulk", fail_fetch)

    summary = await flows.eod_price_flow.fn(trade_date=TO_DATE, provider_exchange_codes=["US"])

    assert summary["exchange"] == {"US": {"bar_date": TO_DATE.isoformat(), "rows_written": 0}}
    assert run.units[0]["status"] == "skipped"
    assert run.units[0]["reason"] == "already_ingested"
    assert run.completed_summary == summary


@pytest.mark.asyncio
async def test_eod_daily_provider_latest_still_fetches_without_trade_date(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provider-latest daily runs cannot pre-skip because the bar date is unknown before fetch."""
    run = FakeRun()
    calls: list[str] = []

    def fail_precheck(*_: object) -> bool:
        raise AssertionError("trade_date=None should not call the explicit-date precheck")

    async def fetch(**kwargs: object) -> list[object]:
        calls.append(str(kwargs["provider_exchange_code"]))
        return []

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "eod_price_already_ingested", fail_precheck)
    monkeypatch.setattr(flows, "fetch_eod_price_bulk", fetch)

    summary = await flows.eod_price_flow.fn(provider_exchange_codes=["US"])

    assert calls == ["US"]
    assert summary["exchange"] == {"US": {"bar_date": None, "rows_written": 0}}
    assert run.units[0]["reason"] == "no_data"


@pytest.mark.asyncio
async def test_eod_daily_coverage_gate_marks_run_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    """A clean daily ingest should be audited partial when rebuilt coverage still has gaps."""
    run = FakeRun()

    async def fetch(**_: object) -> list[object]:
        return [object()]

    async def write_landing(*_: object, **__: object) -> LandingWrite:
        return LandingWrite(
            dataset="eod_price.daily",
            source_uri="s3://bucket/daily.jsonl",
            partition={"provider_exchange_code": "US", "bar_date": TO_DATE},
            rows_raw=1,
        )

    async def dbt_build(**_: object) -> dict[str, object]:
        return {"enabled": True, "triggered": True, "build": "price-build"}

    def load_gaps(**kwargs: object) -> list[dict[str, object]]:
        assert kwargs["exchange_dates"] == {"US": TO_DATE}
        return [
            {
                "data_provider": "eodhd",
                "provider_exchange_code": "US",
                "bar_date": TO_DATE,
                "exchange_day_status": "missing_price",
                "expected_instruments": 2,
                "priced_instruments": 1,
                "missing_price_instruments": 1,
                "known_no_data_instruments": 0,
                "unknown_calendar_instruments": 0,
                "unknown_calendar_coverage_instruments": 0,
                "unknown_instrument_lifecycle_instruments": 0,
            }
        ]

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "eod_price_already_ingested", lambda *_: False)
    monkeypatch.setattr(flows, "fetch_eod_price_bulk", fetch)
    monkeypatch.setattr(flows, "write_eod_price_to_landing", write_landing)
    monkeypatch.setattr(flows, "parse_eod_price", lambda *_args, **_kwargs: ([object()], []))
    monkeypatch.setattr(flows, "write_bronze_eod_price", lambda *_args, **_kwargs: BronzeWrite(rows_written=1))
    monkeypatch.setattr(flows, "run_dbt_build_after_ingestion", dbt_build)
    monkeypatch.setattr(flows, "load_eod_price_coverage_gaps", load_gaps)

    summary = await flows.eod_price_flow.fn(trade_date=TO_DATE, provider_exchange_codes=["US"], run_dbt_build=True)
    coverage_gate = cast(dict[str, object], summary["coverage_gate"])

    assert run.completed_status == "partial"
    assert coverage_gate["status"] == "failed"
    assert coverage_gate["by_status"] == {"missing_price": 1}


@pytest.mark.asyncio
async def test_eod_daily_dbt_failure_preserves_ingestion_audit_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """A post-ingestion dbt exception should complete the app run, then fail Prefect."""
    run = FakeRun()

    async def fetch(**_: object) -> list[object]:
        return [object()]

    async def write_landing(*_: object, **__: object) -> LandingWrite:
        return LandingWrite(
            dataset="eod_price.daily",
            source_uri="s3://bucket/daily.jsonl",
            partition={"provider_exchange_code": "US", "bar_date": TO_DATE},
            rows_raw=1,
        )

    async def dbt_build(**_: object) -> dict[str, object]:
        raise RuntimeError("dbt failed")

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "eod_price_already_ingested", lambda *_: False)
    monkeypatch.setattr(flows, "fetch_eod_price_bulk", fetch)
    monkeypatch.setattr(flows, "write_eod_price_to_landing", write_landing)
    monkeypatch.setattr(flows, "parse_eod_price", lambda *_args, **_kwargs: ([object()], []))
    monkeypatch.setattr(flows, "write_bronze_eod_price", lambda *_args, **_kwargs: BronzeWrite(rows_written=1))
    monkeypatch.setattr(flows, "run_dbt_build_after_ingestion", dbt_build)

    with pytest.raises(RuntimeError, match="dbt failed"):
        await flows.eod_price_flow.fn(trade_date=TO_DATE, provider_exchange_codes=["US"], run_dbt_build=True)

    assert run.completed_status == "completed"
    assert run.failed_error is None
    assert run.completed_summary is not None
    assert run.completed_summary["post_ingestion_error"] == {"type": "RuntimeError", "message": "dbt failed"}


@pytest.mark.asyncio
async def test_eod_daily_no_data_still_runs_coverage_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit daily no-data skips should still be audited partial when coverage has gaps."""
    run = FakeRun()
    build_calls: list[dict[str, object]] = []

    async def fetch(**_: object) -> list[object]:
        return []

    async def dbt_build(**kwargs: object) -> dict[str, object]:
        build_calls.append(kwargs)
        return {"enabled": True, "triggered": True, "build": "price-build"}

    def load_gaps(**kwargs: object) -> list[dict[str, object]]:
        assert kwargs["exchange_dates"] == {"US": TO_DATE}
        return [
            {
                "data_provider": "eodhd",
                "provider_exchange_code": "US",
                "bar_date": TO_DATE,
                "exchange_day_status": "missing_price",
                "expected_instruments": 2,
                "priced_instruments": 0,
                "missing_price_instruments": 2,
                "known_no_data_instruments": 0,
                "unknown_calendar_instruments": 0,
                "unknown_calendar_coverage_instruments": 0,
                "unknown_instrument_lifecycle_instruments": 0,
            }
        ]

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "eod_price_already_ingested", lambda *_: False)
    monkeypatch.setattr(flows, "fetch_eod_price_bulk", fetch)
    monkeypatch.setattr(flows, "run_dbt_build_deployment", dbt_build)
    monkeypatch.setattr(flows, "load_eod_price_coverage_gaps", load_gaps)

    summary = await flows.eod_price_flow.fn(trade_date=TO_DATE, provider_exchange_codes=["US"], run_dbt_build=True)
    coverage_gate = cast(dict[str, object], summary["coverage_gate"])

    assert build_calls == [
        {
            "build": "price-build",
            "parent_run_id": "run-1",
            "idempotency_key": "run-1:price-build",
            "tags": ["post-ingestion-dbt", "price-build"],
        }
    ]
    assert run.completed_status == "partial"
    assert coverage_gate["status"] == "failed"


@pytest.mark.asyncio
async def test_eod_daily_provider_latest_date_mismatch_is_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provider-latest daily runs should not silently accept stale provider dates."""
    run = FakeRun()
    returned_date = date(2026, 5, 30)

    async def fetch(**_: object) -> list[EODBulkPriceRaw]:
        return [_bulk_row(row_date=returned_date)]

    async def write_landing(*_: object, **__: object) -> LandingWrite:
        return LandingWrite(
            dataset="eod_price.daily",
            source_uri="s3://bucket/daily.jsonl",
            partition={"provider_exchange_code": "US", "bar_date": returned_date},
            rows_raw=1,
        )

    async def dbt_build(**_: object) -> dict[str, object]:
        return {"enabled": True, "triggered": True, "build": "price-build"}

    gap_calls: list[dict[str, object]] = []

    def load_gaps(**kwargs: object) -> list[dict[str, object]]:
        gap_calls.append(kwargs)
        return []

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "load_eod_latest_expected_exchange_dates", lambda *_: {"US": TO_DATE})
    monkeypatch.setattr(flows, "fetch_eod_price_bulk", fetch)
    monkeypatch.setattr(flows, "write_eod_price_to_landing", write_landing)
    monkeypatch.setattr(flows, "parse_eod_price", lambda *_args, **_kwargs: ([object()], []))
    monkeypatch.setattr(flows, "write_bronze_eod_price", lambda *_args, **_kwargs: BronzeWrite(rows_written=1))
    monkeypatch.setattr(flows, "run_dbt_build_after_ingestion", dbt_build)
    monkeypatch.setattr(flows, "load_eod_price_coverage_gaps", load_gaps)

    summary = await flows.eod_price_flow.fn(provider_exchange_codes=["US"], run_dbt_build=True)

    assert run.completed_status == "partial"
    assert summary["latest_date_mismatches"] == [
        {
            "provider_exchange_code": "US",
            "provider_bar_date": returned_date.isoformat(),
            "expected_bar_date": TO_DATE.isoformat(),
        }
    ]
    assert gap_calls[0]["exchange_dates"] == {"US": TO_DATE}


@pytest.mark.asyncio
async def test_eod_backfill_provider_call_budget_limits_submitted_instruments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """max_provider_calls caps scheduled per-instrument fetches so a later run can resume from Bronze."""
    run = FakeRun()
    calls: list[str] = []

    async def fetch(
        provider_exchange_code: str,
        provider_instrument_code: str,
        _: date,
        __: date,
    ) -> list[EODPriceBarRaw]:
        calls.append(f"{provider_exchange_code}:{provider_instrument_code}")
        return []

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "load_backfill_pending_instruments", lambda *_: ["AAPL", "MSFT", "GOOG"])
    monkeypatch.setattr(flows, "fetch_instrument_eod_history", fetch)
    monkeypatch.setattr(flows, "write_instrument_eod_history_to_landing", _write_empty_landing)
    coverage_calls: list[str] = []
    deferred_calls: list[list[str]] = []

    def record_coverage(**kwargs: object) -> BronzeWrite:
        coverage_calls.append(str(kwargs["provider_instrument_code"]))
        return BronzeWrite(rows_written=1)

    def record_deferred(**kwargs: object) -> BronzeWrite:
        instruments = list(cast(list[str], kwargs["provider_instrument_codes"]))
        deferred_calls.append(instruments)
        return BronzeWrite(rows_written=len(instruments))

    monkeypatch.setattr(flows, "write_eod_backfill_coverage", record_coverage)
    monkeypatch.setattr(flows, "write_eod_backfill_deferred_coverage", record_deferred)

    summary = await flows.eod_price_backfill_flow.fn(
        from_date=FROM_DATE,
        to_date=TO_DATE,
        provider_exchange_codes=["US"],
        batch_size=50,
        max_provider_calls=1,
    )

    assert coverage_calls == ["AAPL"]
    assert deferred_calls == [["MSFT", "GOOG"]]

    assert calls == ["US:AAPL"]
    assert summary["provider_quota_exhausted"] is True
    assert summary["provider_calls"] == {"max": 1, "submitted": 1, "deferred": 2}
    exchange = cast(dict[str, dict[str, object]], summary["exchange"])
    assert exchange["US"]["instruments_pending"] == 3
    assert exchange["US"]["instruments_deferred"] == 2
    assert run.completed_summary == summary


@pytest.mark.asyncio
async def test_eod_backfill_defaults_to_full_history_and_records_completed_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Omitting from_date should request provider full history and mark the window completed."""
    run = FakeRun()
    pending_calls: list[tuple[str, date | None, date]] = []
    fetch_calls: list[tuple[str, str, date | None, date]] = []
    completed_calls: list[dict[str, object]] = []

    def load_pending(provider_exchange_code: str, from_date: date | None, to_date: date) -> list[str]:
        pending_calls.append((provider_exchange_code, from_date, to_date))
        return ["AAPL"]

    async def fetch(
        provider_exchange_code: str,
        provider_instrument_code: str,
        from_date: date | None,
        to_date: date,
    ) -> list[EODPriceBarRaw]:
        fetch_calls.append((provider_exchange_code, provider_instrument_code, from_date, to_date))
        return [_valid_bar()]

    def write_completed(**kwargs: object) -> BronzeWrite:
        completed_calls.append(kwargs)
        outcomes = cast(list[dict[str, object]], kwargs["outcomes"])
        return BronzeWrite(rows_written=len(outcomes))

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "load_backfill_pending_instruments", load_pending)
    monkeypatch.setattr(flows, "fetch_instrument_eod_history", fetch)
    monkeypatch.setattr(flows, "write_instrument_eod_history_to_landing", _write_empty_landing)
    monkeypatch.setattr(flows, "write_backfill_eod_batch", lambda sources, **_: BronzeWrite(rows_written=len(sources)))
    monkeypatch.setattr(flows, "write_eod_backfill_completed_coverage", write_completed)

    summary = await flows.eod_price_backfill_flow.fn(
        to_date=TO_DATE,
        provider_exchange_codes=["US"],
        batch_size=1,
    )

    assert summary["from_date"] is None
    assert pending_calls == [("US", None, TO_DATE)]
    assert fetch_calls == [("US", "AAPL", None, TO_DATE)]
    unit_key = cast(dict[str, object], run.units[0]["unit_key"])
    assert unit_key["from_date"] is None
    assert completed_calls == [
        {
            "run_id": "run-1",
            "provider_exchange_code": "US",
            "from_date": None,
            "to_date": TO_DATE,
            "outcomes": [
                {
                    "provider_instrument_code": "AAPL",
                    "rows_raw": 1,
                    "rows_valid": 1,
                    "rows_rejected": 0,
                    "source_uri": "s3://bucket/eod_price/US/AAPL.jsonl",
                }
            ],
        }
    ]


@pytest.mark.asyncio
async def test_eod_backfill_builds_missing_selection_views_before_pending_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Historical backfill can bootstrap missing Silver selector views before selection."""
    run = FakeRun()
    missing_responses = [
        [
            "int_exchange_trading_day",
            "int_eod_price_instrument_day_coverage",
            "int_eod_price_backfill_terminal_coverage",
        ],
        [],
    ]
    pending_calls: list[str] = []
    build_calls: list[dict[str, object]] = []

    def load_missing_views() -> list[str]:
        return missing_responses.pop(0)

    async def build_price(**kwargs: object) -> dict[str, object]:
        build_calls.append(kwargs)
        return {
            "enabled": True,
            "triggered": True,
            "build": kwargs["build"],
            "deployment": "dbt-build/price-build",
        }

    def load_pending(provider_exchange_code: str, _: date, __: date) -> list[str]:
        pending_calls.append(provider_exchange_code)
        return []

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "load_missing_eod_backfill_selection_views", load_missing_views)
    monkeypatch.setattr(flows, "run_dbt_build_deployment", build_price)
    monkeypatch.setattr(flows, "load_backfill_pending_instruments", load_pending)

    summary = await flows.eod_price_backfill_flow.fn(
        from_date=FROM_DATE,
        to_date=TO_DATE,
        provider_exchange_codes=["US"],
        build_selection_views_if_missing=True,
    )

    assert build_calls == [
        {
            "build": "price-build",
            "parent_run_id": "run-1",
            "idempotency_key": "run-1:preflight:price-build",
            "tags": ["preflight-dbt", "price-build"],
        }
    ]
    assert pending_calls == ["US"]
    assert summary["preflight_dbt_build"] == {
        "enabled": True,
        "triggered": True,
        "build": "price-build",
        "deployment": "dbt-build/price-build",
        "reason": "missing_selection_views",
        "missing_before": [
            "int_exchange_trading_day",
            "int_eod_price_instrument_day_coverage",
            "int_eod_price_backfill_terminal_coverage",
        ],
        "missing_after": [],
    }
    assert summary["exchange"] == {"US": {"instruments": 0, "rows": 0}}


@pytest.mark.asyncio
async def test_eod_backfill_preflight_failure_is_audited(monkeypatch: pytest.MonkeyPatch) -> None:
    """Preflight dbt failures should fail the tracked app run with preflight context."""
    run = FakeRun()
    build_calls: list[dict[str, object]] = []

    def load_missing_views() -> list[str]:
        return ["int_eod_price_backfill_terminal_coverage"]

    async def build_price(**kwargs: object) -> dict[str, object]:
        build_calls.append(kwargs)
        raise RuntimeError("preflight dbt failed")

    def fail_pending(*_: object) -> list[str]:
        raise AssertionError("pending selection should not run after preflight failure")

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "load_missing_eod_backfill_selection_views", load_missing_views)
    monkeypatch.setattr(flows, "run_dbt_build_deployment", build_price)
    monkeypatch.setattr(flows, "load_backfill_pending_instruments", fail_pending)

    with pytest.raises(RuntimeError, match="preflight dbt failed"):
        await flows.eod_price_backfill_flow.fn(
            from_date=FROM_DATE,
            to_date=TO_DATE,
            provider_exchange_codes=["US"],
            build_selection_views_if_missing=True,
        )

    assert build_calls == [
        {
            "build": "price-build",
            "parent_run_id": "run-1",
            "idempotency_key": "run-1:preflight:price-build",
            "tags": ["preflight-dbt", "price-build"],
        }
    ]
    assert run.failed_summary is not None
    preflight = cast(dict[str, object], run.failed_summary["preflight_dbt_build"])
    assert preflight["status"] == "failed"
    assert preflight["error"] == {"type": "RuntimeError", "message": "preflight dbt failed"}


@pytest.mark.asyncio
async def test_eod_backfill_stops_after_provider_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 429 stops later backfill batches instead of failing every remaining instrument."""
    run = FakeRun()
    calls: list[str] = []

    async def fetch(
        provider_exchange_code: str,
        provider_instrument_code: str,
        _: date,
        __: date,
    ) -> list[EODPriceBarRaw]:
        calls.append(f"{provider_exchange_code}:{provider_instrument_code}")
        if provider_instrument_code == "MSFT":
            raise _provider_rate_limit_error(f"{provider_instrument_code}.{provider_exchange_code}")
        return []

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "load_backfill_pending_instruments", lambda *_: ["AAPL", "MSFT", "GOOG"])
    monkeypatch.setattr(flows, "fetch_instrument_eod_history", fetch)
    monkeypatch.setattr(flows, "write_instrument_eod_history_to_landing", _write_empty_landing)
    monkeypatch.setattr(
        flows,
        "write_eod_backfill_coverage",
        lambda **_: BronzeWrite(rows_written=1),
    )
    deferred_calls: list[list[str]] = []

    def record_deferred(**kwargs: object) -> BronzeWrite:
        instruments = list(cast(list[str], kwargs["provider_instrument_codes"]))
        deferred_calls.append(instruments)
        return BronzeWrite(rows_written=len(instruments))

    monkeypatch.setattr(flows, "write_eod_backfill_deferred_coverage", record_deferred)

    summary = await flows.eod_price_backfill_flow.fn(
        from_date=FROM_DATE,
        to_date=TO_DATE,
        provider_exchange_codes=["US"],
        batch_size=1,
    )

    assert calls == ["US:AAPL", "US:MSFT"]
    assert summary["provider_quota_exhausted"] is True
    assert summary["failed_instruments"] == ["MSFT"]
    assert deferred_calls == [["GOOG"]]
    exchange = cast(dict[str, dict[str, object]], summary["exchange"])
    assert exchange["US"]["instruments_deferred"] == 1
    assert [unit.get("reason") for unit in run.units] == [
        "no_valid_rows",
        "provider_rate_limited",
        "instrument_failures",
    ]


@pytest.mark.asyncio
async def test_eod_backfill_all_rejected_rows_do_not_write_no_data_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parser failures should remain retryable instead of being marked terminal no_data."""
    run = FakeRun()

    async def fetch(_: str, __: str, ___: date, ____: date) -> list[EODPriceBarRaw]:
        return [_rejected_bar()]

    def fail_coverage(**_: object) -> BronzeWrite:
        raise AssertionError("all rejected rows should not be recorded as no_data coverage")

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "load_backfill_pending_instruments", lambda *_: ["AAPL"])
    monkeypatch.setattr(flows, "fetch_instrument_eod_history", fetch)
    monkeypatch.setattr(flows, "write_instrument_eod_history_to_landing", _write_empty_landing)
    monkeypatch.setattr(flows, "write_eod_backfill_coverage", fail_coverage)
    monkeypatch.setattr(
        flows,
        "write_eod_backfill_deferred_coverage",
        lambda **_: BronzeWrite(rows_written=0),
    )

    summary = await flows.eod_price_backfill_flow.fn(
        from_date=FROM_DATE,
        to_date=TO_DATE,
        provider_exchange_codes=["US"],
        batch_size=1,
    )

    assert summary["provider_quota_exhausted"] is False
    assert [unit.get("reason") for unit in run.units] == ["all_rows_rejected", None]


@pytest.mark.asyncio
async def test_eod_backfill_mixed_valid_and_rejected_rows_do_not_write_completed_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mixed parser outcomes should write valid bars but keep the exact window retryable."""
    run = FakeRun()
    completed_calls: list[dict[str, object]] = []

    async def fetch(_: str, __: str, ___: date, ____: date) -> list[EODPriceBarRaw]:
        return [_valid_bar(), _rejected_bar()]

    def fail_no_data_coverage(**_: object) -> BronzeWrite:
        raise AssertionError("mixed valid/rejected rows should not be marked no_data")

    def record_completed(**kwargs: object) -> BronzeWrite:
        completed_calls.append(kwargs)
        return BronzeWrite(rows_written=1)

    monkeypatch.setattr(flows, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(flows, "load_backfill_pending_instruments", lambda *_: ["AAPL"])
    monkeypatch.setattr(flows, "fetch_instrument_eod_history", fetch)
    monkeypatch.setattr(flows, "write_instrument_eod_history_to_landing", _write_empty_landing)
    monkeypatch.setattr(flows, "write_backfill_eod_batch", lambda sources, **_: BronzeWrite(rows_written=len(sources)))
    monkeypatch.setattr(flows, "write_eod_backfill_coverage", fail_no_data_coverage)
    monkeypatch.setattr(flows, "write_eod_backfill_completed_coverage", record_completed)

    summary = await flows.eod_price_backfill_flow.fn(
        from_date=FROM_DATE,
        to_date=TO_DATE,
        provider_exchange_codes=["US"],
        batch_size=1,
    )

    assert completed_calls == []
    assert summary["failed_instruments"] == []
    assert run.completed_status == "partial"
    assert [unit.get("reason") for unit in run.units] == [None, None]
