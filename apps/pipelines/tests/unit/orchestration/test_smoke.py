"""Tests for local smoke preset runner."""

from datetime import date

import pytest
from pytest import MonkeyPatch

from orchestration import smoke


@pytest.mark.asyncio
async def test_run_preset_rejects_production(monkeypatch: MonkeyPatch) -> None:
    """Smoke presets should not run in production environments."""

    class FakeSettings:
        @property
        def is_production(self) -> bool:
            return True

    monkeypatch.setattr(smoke, "get_settings", lambda: FakeSettings())

    with pytest.raises(RuntimeError, match="local/dev tool"):
        await smoke.run_preset(
            smoke.SmokePresetRequest(
                preset="fundamental",
                exchange="US",
                instrument="AAPL",
            )
        )


@pytest.mark.asyncio
async def test_run_preset_fundamental_defaults(monkeypatch: MonkeyPatch) -> None:
    """Fundamental smoke should run one explicit instrument with a one-call credit cap."""
    calls: list[dict[str, object]] = []

    async def fake_fundamental_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(smoke, "fundamental_flow", fake_fundamental_flow)

    summary = await smoke.run_preset(
        smoke.SmokePresetRequest(
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
async def test_run_preset_fundamental_overrides(monkeypatch: MonkeyPatch) -> None:
    """Fundamental smoke should pass explicit instrument and snapshot date overrides."""
    calls: list[dict[str, object]] = []

    async def fake_fundamental_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(smoke, "fundamental_flow", fake_fundamental_flow)

    summary = await smoke.run_preset(
        smoke.SmokePresetRequest(
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
async def test_run_preset_instrument_defaults(monkeypatch: MonkeyPatch) -> None:
    """Instrument smoke should run one provider namespace."""
    calls: list[dict[str, object]] = []

    async def fake_instrument_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(smoke, "instrument_flow", fake_instrument_flow)

    summary = await smoke.run_preset(
        smoke.SmokePresetRequest(
            preset="instrument",
            exchange="XETRA",
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_exchange_codes": ["XETRA"], "snapshot_date": None}]


@pytest.mark.asyncio
async def test_run_preset_instrument_overrides(monkeypatch: MonkeyPatch) -> None:
    """Instrument smoke should pass exchange and snapshot date overrides."""
    calls: list[dict[str, object]] = []

    async def fake_instrument_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(smoke, "instrument_flow", fake_instrument_flow)

    summary = await smoke.run_preset(
        smoke.SmokePresetRequest(
            preset="instrument",
            exchange="US",
            snapshot_date=date(2026, 5, 31),
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_exchange_codes": ["US"], "snapshot_date": date(2026, 5, 31)}]


@pytest.mark.asyncio
async def test_run_preset_exchange_runs_catalog_and_mic(monkeypatch: MonkeyPatch) -> None:
    """Exchange smoke should run both exchange reference refreshes."""
    calls: list[str] = []

    async def fake_exchange_catalog_flow() -> int:
        calls.append("catalog")
        return 12

    async def fake_exchange_mic_registry_flow() -> dict[str, object]:
        calls.append("mic")
        return {"rows_written": 34}

    monkeypatch.setattr(smoke, "exchange_catalog_flow", fake_exchange_catalog_flow)
    monkeypatch.setattr(smoke, "exchange_mic_registry_flow", fake_exchange_mic_registry_flow)

    summary = await smoke.run_preset(smoke.SmokePresetRequest(preset="exchange"))

    assert calls == ["catalog", "mic"]
    assert summary == {
        "exchange_catalog_rows_written": 12,
        "exchange_mic_registry": {"rows_written": 34},
    }


@pytest.mark.asyncio
async def test_run_preset_exchange_schedule_defaults(monkeypatch: MonkeyPatch) -> None:
    """Exchange schedule smoke should run one provider schedule namespace."""
    calls: list[dict[str, object]] = []

    async def fake_exchange_schedule_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(smoke, "exchange_schedule_flow", fake_exchange_schedule_flow)

    summary = await smoke.run_preset(
        smoke.SmokePresetRequest(
            preset="exchange-schedule",
            exchange="US",
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_schedule_exchange_codes": ["US"], "snapshot_date": None}]


@pytest.mark.asyncio
async def test_run_preset_exchange_schedule_overrides(monkeypatch: MonkeyPatch) -> None:
    """Exchange schedule smoke should pass exchange and snapshot date overrides."""
    calls: list[dict[str, object]] = []

    async def fake_exchange_schedule_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(smoke, "exchange_schedule_flow", fake_exchange_schedule_flow)

    summary = await smoke.run_preset(
        smoke.SmokePresetRequest(
            preset="exchange-schedule",
            exchange="XETR",
            snapshot_date=date(2026, 5, 31),
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_schedule_exchange_codes": ["XETR"], "snapshot_date": date(2026, 5, 31)}]


@pytest.mark.asyncio
async def test_run_preset_eod_price_defaults(monkeypatch: MonkeyPatch) -> None:
    """EOD price smoke should run one provider namespace."""
    calls: list[dict[str, object]] = []

    async def fake_eod_price_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(smoke, "eod_price_flow", fake_eod_price_flow)

    summary = await smoke.run_preset(
        smoke.SmokePresetRequest(
            preset="eod-price",
            exchange="XETRA",
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_exchange_codes": ["XETRA"], "trade_date": None}]


@pytest.mark.asyncio
async def test_run_preset_eod_price_overrides(monkeypatch: MonkeyPatch) -> None:
    """EOD price smoke should pass exchange and trade date overrides."""
    calls: list[dict[str, object]] = []

    async def fake_eod_price_flow(**kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return {"ok": True}

    monkeypatch.setattr(smoke, "eod_price_flow", fake_eod_price_flow)

    summary = await smoke.run_preset(
        smoke.SmokePresetRequest(
            preset="eod-price",
            exchange="US",
            trade_date=date(2026, 5, 31),
        )
    )

    assert summary == {"ok": True}
    assert calls == [{"provider_exchange_codes": ["US"], "trade_date": date(2026, 5, 31)}]


def test_eod_price_accepts_domain_style_alias() -> None:
    """EOD price smoke should accept the Python domain spelling too."""
    assert smoke.normalize_preset("eod_price") == "eod-price"


def test_exchange_schedule_accepts_domain_style_alias() -> None:
    """Exchange schedule smoke should accept the Python domain spelling too."""
    assert smoke.normalize_preset("exchange_schedule") == "exchange-schedule"
