"""Tests for instrument ingestion service."""

from collections.abc import Generator
from contextlib import contextmanager
from datetime import date
from typing import cast

import pytest

from core.ingestion import BronzeWrite, LandingWrite, RunUnitTally
from core.ingestion.run_tracking import UnitStatus
from domains.instrument import service
from domains.instrument.contracts import InstrumentRefreshRequest


class FakeRun:
    """Minimal run scope double for instrument service tests."""

    def __init__(self) -> None:
        """Create a fake audit run."""
        self.run_id = "instrument-run"
        self.tally = RunUnitTally()
        self.is_terminal = False
        self.completed_kwargs: dict[str, object] | None = None
        self.failed_error: object | None = None

    def record_unit(self, **kwargs: object) -> str:
        """Capture one unit and update the tally."""
        status = kwargs.get("status")
        if isinstance(status, str):
            self.tally.record(cast(UnitStatus, status))
        return "unit-1"

    def record_unit_with_landing(self, _: LandingWrite, **kwargs: object) -> str:
        """Capture one landed unit and update the tally."""
        return self.record_unit(**kwargs)

    def complete(self, **kwargs: object) -> None:
        """Mark the fake run terminal."""
        self.completed_kwargs = kwargs
        self.is_terminal = True

    def fail(self, error: object, **__: object) -> None:
        """Mark the fake run failed."""
        self.failed_error = error
        self.is_terminal = True


class FakeTracker:
    """Minimal tracker double that yields one fake run."""

    def __init__(self, run: FakeRun) -> None:
        """Create the tracker."""
        self.run = run

    @contextmanager
    def track_run(self, **_: object) -> Generator[FakeRun]:
        """Yield the fake run."""
        yield self.run


@pytest.mark.asyncio
async def test_run_instrument_refresh_writes_landing_and_bronze(monkeypatch: pytest.MonkeyPatch) -> None:
    """Instrument service should own ingestion control and return durable run metadata."""
    run = FakeRun()
    materializations: list[dict[str, object]] = []
    published: list[dict[str, object]] = []

    async def fake_fetch(_: str) -> list[dict[str, str]]:
        return [{"Code": "AAPL"}]

    async def fake_landing(_: object, provider_exchange_code: str, snapshot_date: date) -> LandingWrite:
        return LandingWrite(
            dataset="instrument.symbols",
            source_uri=f"s3://bucket/instrument/{provider_exchange_code}.json",
            partition={"provider_exchange_code": provider_exchange_code, "snapshot_date": snapshot_date},
            rows_raw=1,
        )

    async def fake_publish(**kwargs: object) -> None:
        published.append(kwargs)

    monkeypatch.setattr(service, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(service, "instrument_already_ingested", lambda *_: False)
    monkeypatch.setattr(service, "fetch_instrument", fake_fetch)
    monkeypatch.setattr(service, "write_instrument_to_landing_zone", fake_landing)
    monkeypatch.setattr(service, "write_bronze_instrument", lambda *_args, **_kwargs: BronzeWrite(rows_written=1))
    monkeypatch.setattr(
        service,
        "record_instrument_bronze_materialization",
        lambda **kwargs: materializations.append(kwargs),
    )
    monkeypatch.setattr(service, "publish_prefect_ingestion_summary", fake_publish)

    result = await service.run_instrument_refresh(
        InstrumentRefreshRequest(
            snapshot_date=date(2026, 6, 1),
            provider_exchange_codes=["US"],
        )
    )

    assert result.run_id == "instrument-run"
    assert result.status == "completed"
    assert result.summary["exchange"] == {"US": {"rows": 1, "raw_rows": 1}}
    assert materializations == [
        {
            "app_run_id": "instrument-run",
            "snapshot_date": "2026-06-01",
            "provider": "eodhd",
            "rows_written": 1,
        }
    ]
    assert published[0]["status"] == "completed"


@pytest.mark.asyncio
async def test_run_instrument_refresh_respects_empty_exchange_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit empty exchange list should be a no-op, not a request for all exchanges."""
    run = FakeRun()
    published: list[dict[str, object]] = []

    async def unexpected_default_codes() -> list[str]:
        raise AssertionError("default exchange codes should not be loaded")

    async def fake_publish(**kwargs: object) -> None:
        published.append(kwargs)

    monkeypatch.setattr(service, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(service, "fetch_instrument_provider_exchange_codes", unexpected_default_codes)
    monkeypatch.setattr(service, "instrument_already_ingested", lambda *_: pytest.fail("no exchange checks expected"))
    monkeypatch.setattr(service, "fetch_instrument", lambda *_: pytest.fail("no provider fetches expected"))
    monkeypatch.setattr(service, "publish_prefect_ingestion_summary", fake_publish)

    result = await service.run_instrument_refresh(
        InstrumentRefreshRequest(
            snapshot_date=date(2026, 6, 1),
            provider_exchange_codes=[],
        )
    )

    assert result.status == "skipped"
    assert result.summary["exchange"] == {}
    assert result.summary["skipped"] == []
    assert result.summary["failed"] == []
    assert run.completed_kwargs is not None
    assert run.completed_kwargs["status"] == "skipped"
    assert published[0]["status"] == "skipped"


@pytest.mark.asyncio
async def test_run_instrument_refresh_audits_default_scope_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Failures while resolving default exchange scope should be recorded in the run audit."""
    run = FakeRun()
    published: list[dict[str, object]] = []

    async def failing_default_codes() -> list[str]:
        raise RuntimeError("exchange universe unavailable")

    async def fake_publish(**kwargs: object) -> None:
        published.append(kwargs)

    monkeypatch.setattr(service, "PipelineRunTracker", lambda: FakeTracker(run))
    monkeypatch.setattr(service, "fetch_instrument_provider_exchange_codes", failing_default_codes)
    monkeypatch.setattr(service, "publish_prefect_ingestion_summary", fake_publish)

    with pytest.raises(RuntimeError, match="exchange universe unavailable"):
        await service.run_instrument_refresh(
            InstrumentRefreshRequest(
                snapshot_date=date(2026, 6, 1),
            )
        )

    assert run.failed_error is not None
    assert "exchange universe unavailable" in str(run.failed_error)
    assert published[0]["status"] == "failed"
