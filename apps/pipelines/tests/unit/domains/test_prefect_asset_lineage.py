"""Tests for domain-owned Prefect asset lineage."""

from domains.eod_price.assets import BRONZE_EOD_PRICE_ASSET, _record_dbt_price_materializations
from domains.exchange.assets import (
    BRONZE_EXCHANGE_CATALOG_ASSET,
    BRONZE_EXCHANGE_MIC_REGISTRY_ASSET,
    _record_dbt_exchange_materializations,
)
from domains.exchange_schedule.assets import (
    BRONZE_EXCHANGE_HOLIDAY_ASSET,
    BRONZE_EXCHANGE_SCHEDULE_ASSET,
    _record_dbt_exchange_schedule_materializations,
)
from domains.fundamental.assets import BRONZE_FUNDAMENTAL_ASSET, _record_dbt_fundamental_materializations
from domains.instrument.assets import BRONZE_INSTRUMENT_ASSET, _record_dbt_instrument_materializations


def test_dbt_materializations_depend_on_domain_bronze_assets() -> None:
    """Silver/Gold dbt assets should point back to the Bronze assets they consume."""
    assert _record_dbt_price_materializations.asset_deps == [BRONZE_EOD_PRICE_ASSET]
    assert _record_dbt_fundamental_materializations.asset_deps == [BRONZE_FUNDAMENTAL_ASSET]
    assert _record_dbt_instrument_materializations.asset_deps == [BRONZE_INSTRUMENT_ASSET]
    assert _record_dbt_exchange_materializations.asset_deps == [
        BRONZE_EXCHANGE_CATALOG_ASSET,
        BRONZE_EXCHANGE_MIC_REGISTRY_ASSET,
    ]
    assert _record_dbt_exchange_schedule_materializations.asset_deps == [
        BRONZE_EXCHANGE_SCHEDULE_ASSET,
        BRONZE_EXCHANGE_HOLIDAY_ASSET,
    ]
