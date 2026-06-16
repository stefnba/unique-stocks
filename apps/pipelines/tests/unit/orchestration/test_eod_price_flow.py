"""Tests for EOD price Prefect flow wiring."""

from datetime import date

import pytest

from domains.eod_price.contracts import EodPriceBackfillRequest, EodPriceDailyRequest, EodPriceRefreshResult
from orchestration.flows import eod_price as eod_price_flows


@pytest.mark.asyncio
async def test_daily_flow_supplies_post_ingestion_hook_when_dbt_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """The daily flow should keep dbt orchestration outside the domain request."""
    calls: list[dict[str, object]] = []

    async def fake_refresh(
        request: EodPriceDailyRequest,
        *,
        post_ingestion: object | None = None,
    ) -> EodPriceRefreshResult:
        calls.append({"request": request, "post_ingestion": post_ingestion})
        return EodPriceRefreshResult(run_id="run-1", status="completed", summary={"ok": True})

    monkeypatch.setattr(eod_price_flows, "run_eod_price_daily", fake_refresh)

    summary = await eod_price_flows.eod_price_flow.fn(
        trade_date=date(2026, 5, 31),
        provider_exchange_codes=["US"],
        run_dbt_build=True,
    )

    assert summary == {"ok": True}
    assert calls == [
        {
            "request": EodPriceDailyRequest(
                trade_date=date(2026, 5, 31),
                provider_exchange_codes=["US"],
            ),
            "post_ingestion": eod_price_flows.run_price_post_ingestion_checks,
        }
    ]
    assert not hasattr(calls[0]["request"], "run_dbt_build")


@pytest.mark.asyncio
async def test_backfill_flow_supplies_preflight_and_post_ingestion_hooks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The backfill flow should own dbt preflight and post-ingestion wiring."""
    calls: list[dict[str, object]] = []

    async def fake_refresh(
        request: EodPriceBackfillRequest,
        *,
        preflight: object | None = None,
        post_ingestion: object | None = None,
    ) -> EodPriceRefreshResult:
        calls.append({"request": request, "preflight": preflight, "post_ingestion": post_ingestion})
        return EodPriceRefreshResult(run_id="run-1", status="completed", summary={"ok": True})

    monkeypatch.setattr(eod_price_flows, "run_eod_price_backfill", fake_refresh)

    summary = await eod_price_flows.eod_price_backfill_flow.fn(
        from_date=date(2026, 5, 1),
        to_date=date(2026, 5, 31),
        provider_exchange_codes=["US"],
        batch_size=10,
        max_provider_calls=20,
        build_selection_views_if_missing=True,
        run_dbt_build=True,
    )

    assert summary == {"ok": True}
    assert calls == [
        {
            "request": EodPriceBackfillRequest(
                from_date=date(2026, 5, 1),
                to_date=date(2026, 5, 31),
                provider_exchange_codes=["US"],
                batch_size=10,
                max_provider_calls=20,
            ),
            "preflight": eod_price_flows.build_price_selection_views_if_missing,
            "post_ingestion": eod_price_flows.run_price_post_ingestion_checks,
        }
    ]
    assert not hasattr(calls[0]["request"], "build_selection_views_if_missing")
    assert not hasattr(calls[0]["request"], "run_dbt_build")
