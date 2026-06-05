"""Live smoke tests for curated EODHD provider namespaces."""

from datetime import date
from os import getenv

import pytest

from config.settings import get_settings
from providers.eodhd.client import EODHDClient

pytestmark = [pytest.mark.integration, pytest.mark.live_provider]


@pytest.mark.asyncio
async def test_eodhd_indx_namespace_supports_instruments_and_eod_price() -> None:
    """EODHD should keep serving the curated INDX namespace and GDAXI symbol."""
    if getenv("RUN_LIVE_EODHD_TESTS") != "1":
        pytest.skip("Set RUN_LIVE_EODHD_TESTS=1 to call the live EODHD API.")

    api_key = get_settings().eodhd_api_key.get_secret_value()
    if not api_key:
        pytest.skip("Set EODHD_API_KEY to call the live EODHD API.")

    async with EODHDClient(api_key=api_key) as client:
        instruments = await client.get_instrument("INDX")
        assert any(
            instrument.provider_instrument_code == "GDAXI"
            and instrument.provider_listing_exchange_code == "INDX"
            and instrument.asset_type == "INDEX"
            for instrument in instruments
        )

        bars = await client.get_eod_price_ticker(
            "GDAXI.INDX",
            from_date=date(2026, 5, 28),
            to_date=date(2026, 5, 29),
        )
        assert bars
        assert all(bar.close > 0 for bar in bars)
