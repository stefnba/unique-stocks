"""Tests for local flow-check preset runner."""

from datetime import date

import pytest
from pytest import MonkeyPatch

from config.domains import Domain
from core.orchestration.flow_check import FlowCheckRequest
from orchestration import flow_check


@pytest.mark.asyncio
async def test_run_flow_check_rejects_production(monkeypatch: MonkeyPatch) -> None:
    """Flow-check presets should not run in production environments."""

    class FakeSettings:
        @property
        def is_production(self) -> bool:
            return True

    monkeypatch.setattr(flow_check, "get_settings", lambda: FakeSettings())

    with pytest.raises(RuntimeError, match="local/dev tool"):
        await flow_check.run_flow_check(
            FlowCheckRequest(
                preset="fundamental",
                exchange="US",
                instrument="AAPL",
            )
        )


@pytest.mark.asyncio
async def test_run_flow_check_fundamental_defaults(monkeypatch: MonkeyPatch) -> None:
    """Fundamental flow-check should run one explicit instrument with a one-call credit cap."""
    calls: list[dict[str, object]] = []

    async def fake_fundamental_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(flow_check, "fundamental_flow", fake_fundamental_flow)

    summary = await flow_check.run_flow_check(
        FlowCheckRequest(
            preset="fundamental",
            exchange="US",
            instrument="AAPL",
        )
    )

    assert summary == {"ok": True}
    assert calls == [
        {
            "provider_instruments": [
                {
                    "provider_exchange_code": "US",
                    "provider_instrument_code": "AAPL",
                }
            ],
            "snapshot_date": None,
            "batch_size": 1,
            "max_provider_credits": 10,
        }
    ]


@pytest.mark.asyncio
async def test_run_flow_check_fundamental_overrides(monkeypatch: MonkeyPatch) -> None:
    """Fundamental flow-check should pass explicit instrument and snapshot date overrides."""
    calls: list[dict[str, object]] = []

    async def fake_fundamental_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(flow_check, "fundamental_flow", fake_fundamental_flow)

    summary = await flow_check.run_flow_check(
        FlowCheckRequest(
            preset="fundamental",
            exchange="US",
            instrument="MSFT",
            snapshot_date=date(2026, 5, 31),
        )
    )

    assert summary == {"ok": True}
    assert calls == [
        {
            "provider_instruments": [
                {
                    "provider_exchange_code": "US",
                    "provider_instrument_code": "MSFT",
                }
            ],
            "snapshot_date": date(2026, 5, 31),
            "batch_size": 1,
            "max_provider_credits": 10,
        }
    ]


@pytest.mark.asyncio
async def test_run_flow_check_instrument_defaults(monkeypatch: MonkeyPatch) -> None:
    """Instrument flow-check should run one provider namespace."""
    calls: list[dict[str, object]] = []

    async def fake_instrument_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(flow_check, "instrument_flow", fake_instrument_flow)

    summary = await flow_check.run_flow_check(
        FlowCheckRequest(
            preset="instrument",
            exchange="XETRA",
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_exchange_codes": ["XETRA"], "snapshot_date": None}]


@pytest.mark.asyncio
async def test_run_flow_check_instrument_overrides(monkeypatch: MonkeyPatch) -> None:
    """Instrument flow-check should pass exchange and snapshot date overrides."""
    calls: list[dict[str, object]] = []

    async def fake_instrument_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(flow_check, "instrument_flow", fake_instrument_flow)

    summary = await flow_check.run_flow_check(
        FlowCheckRequest(
            preset="instrument",
            exchange="US",
            snapshot_date=date(2026, 5, 31),
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_exchange_codes": ["US"], "snapshot_date": date(2026, 5, 31)}]


@pytest.mark.asyncio
async def test_run_flow_check_exchange_runs_catalog_and_mic(monkeypatch: MonkeyPatch) -> None:
    """Exchange flow-check should run both exchange reference refreshes."""
    calls: list[str] = []

    async def fake_exchange_catalog_flow() -> int:
        calls.append("catalog")
        return 12

    async def fake_exchange_mic_registry_flow() -> dict[str, object]:
        calls.append("mic")
        return {"rows_written": 34}

    monkeypatch.setattr(flow_check, "exchange_catalog_flow", fake_exchange_catalog_flow)
    monkeypatch.setattr(flow_check, "exchange_mic_registry_flow", fake_exchange_mic_registry_flow)

    summary = await flow_check.run_flow_check(FlowCheckRequest(preset="exchange"))

    assert calls == ["catalog", "mic"]
    assert summary == {
        "exchange_catalog_rows_written": 12,
        "exchange_mic_registry": {"rows_written": 34},
    }


@pytest.mark.asyncio
async def test_run_flow_check_exchange_schedule_defaults(monkeypatch: MonkeyPatch) -> None:
    """Exchange schedule flow-check should run one provider schedule namespace."""
    calls: list[dict[str, object]] = []

    async def fake_exchange_schedule_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(flow_check, "exchange_schedule_flow", fake_exchange_schedule_flow)

    summary = await flow_check.run_flow_check(
        FlowCheckRequest(
            preset="exchange-schedule",
            exchange="US",
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_schedule_exchange_codes": ["US"], "snapshot_date": None}]


@pytest.mark.asyncio
async def test_run_flow_check_exchange_schedule_overrides(monkeypatch: MonkeyPatch) -> None:
    """Exchange schedule flow-check should pass exchange and snapshot date overrides."""
    calls: list[dict[str, object]] = []

    async def fake_exchange_schedule_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(flow_check, "exchange_schedule_flow", fake_exchange_schedule_flow)

    summary = await flow_check.run_flow_check(
        FlowCheckRequest(
            preset="exchange-schedule",
            exchange="XETR",
            snapshot_date=date(2026, 5, 31),
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_schedule_exchange_codes": ["XETR"], "snapshot_date": date(2026, 5, 31)}]


@pytest.mark.asyncio
async def test_run_flow_check_eod_price_defaults(monkeypatch: MonkeyPatch) -> None:
    """EOD price flow-check should run one provider namespace."""
    calls: list[dict[str, object]] = []

    async def fake_eod_price_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(flow_check, "eod_price_flow", fake_eod_price_flow)

    summary = await flow_check.run_flow_check(
        FlowCheckRequest(
            preset="eod-price",
            exchange="XETRA",
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_exchange_codes": ["XETRA"], "trade_date": None}]


@pytest.mark.asyncio
async def test_run_flow_check_eod_price_overrides(monkeypatch: MonkeyPatch) -> None:
    """EOD price flow-check should pass exchange and trade date overrides."""
    calls: list[dict[str, object]] = []

    async def fake_eod_price_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(flow_check, "eod_price_flow", fake_eod_price_flow)

    summary = await flow_check.run_flow_check(
        FlowCheckRequest(
            preset="eod-price",
            exchange="US",
            trade_date=date(2026, 5, 31),
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_exchange_codes": ["US"], "trade_date": date(2026, 5, 31)}]


def test_eod_price_accepts_domain_style_alias() -> None:
    """EOD price flow-check should accept the Python domain spelling too."""
    assert flow_check.normalize_preset("eod_price") == "eod-price"


def test_exchange_schedule_accepts_domain_style_alias() -> None:
    """Exchange schedule flow-check should accept the Python domain spelling too."""
    assert flow_check.normalize_preset("exchange_schedule") == "exchange-schedule"


def test_flow_check_presets_accept_domain_values() -> None:
    """Flow-check aliases should come from the shared domain identities."""
    assert {domain.value: flow_check.normalize_preset(domain.value) for domain in flow_check.FLOW_CHECK_DOMAINS} == {
        Domain.FUNDAMENTAL.value: "fundamental",
        Domain.EXCHANGE.value: "exchange",
        Domain.EXCHANGE_SCHEDULE.value: "exchange-schedule",
        Domain.INSTRUMENT.value: "instrument",
        Domain.EOD_PRICE.value: "eod-price",
    }


def test_normalize_preset_is_case_and_whitespace_tolerant() -> None:
    """Flow-check preset normalization should be forgiving at CLI boundaries."""
    assert flow_check.normalize_preset(" EOD_PRICE ") == "eod-price"
