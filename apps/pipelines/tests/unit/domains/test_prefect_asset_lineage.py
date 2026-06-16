"""Tests for domain-owned Prefect asset lineage."""

from domains.eod_price.assets import (
    BRONZE_EOD_PRICE_ASSET,
    SILVER_PRICE_ASSET,
    _record_gold_price_materialization,
    _record_silver_price_materialization,
    attach_eod_price_bronze_metadata,
)
from domains.exchange.assets import (
    BRONZE_EXCHANGE_CATALOG_ASSET,
    BRONZE_EXCHANGE_MIC_REGISTRY_ASSET,
    SILVER_EXCHANGE_ASSET,
    _record_gold_exchange_materialization,
    _record_silver_exchange_materialization,
)
from domains.exchange_schedule.assets import (
    BRONZE_EXCHANGE_HOLIDAY_ASSET,
    BRONZE_EXCHANGE_SCHEDULE_ASSET,
    SILVER_EXCHANGE_SCHEDULE_ASSET,
    _record_gold_exchange_schedule_materialization,
    _record_silver_exchange_schedule_materialization,
    attach_exchange_holiday_bronze_metadata,
)
from domains.fundamental.assets import (
    BRONZE_FUNDAMENTAL_ASSET,
    SILVER_FUNDAMENTAL_ASSET,
    _record_gold_fundamental_materialization,
    _record_silver_fundamental_materialization,
    attach_fundamental_bronze_metadata,
)
from domains.instrument.assets import (
    BRONZE_INSTRUMENT_ASSET,
    SILVER_INSTRUMENT_ASSET,
    _record_gold_instrument_materialization,
    _record_silver_instrument_materialization,
)


def test_dbt_materializations_depend_on_domain_bronze_assets() -> None:
    """Silver/Gold dbt assets should point back to the Bronze assets they consume."""
    assert _record_silver_price_materialization.asset_deps == [BRONZE_EOD_PRICE_ASSET]
    assert _record_gold_price_materialization.asset_deps == [SILVER_PRICE_ASSET]
    assert _record_silver_fundamental_materialization.asset_deps == [BRONZE_FUNDAMENTAL_ASSET]
    assert _record_gold_fundamental_materialization.asset_deps == [SILVER_FUNDAMENTAL_ASSET]
    assert _record_silver_instrument_materialization.asset_deps == [BRONZE_INSTRUMENT_ASSET]
    assert _record_gold_instrument_materialization.asset_deps == [SILVER_INSTRUMENT_ASSET]
    assert _record_silver_exchange_materialization.asset_deps == [
        BRONZE_EXCHANGE_CATALOG_ASSET,
        BRONZE_EXCHANGE_MIC_REGISTRY_ASSET,
    ]
    assert _record_gold_exchange_materialization.asset_deps == [SILVER_EXCHANGE_ASSET]
    assert _record_silver_exchange_schedule_materialization.asset_deps == [
        BRONZE_EXCHANGE_SCHEDULE_ASSET,
        BRONZE_EXCHANGE_HOLIDAY_ASSET,
    ]
    assert _record_gold_exchange_schedule_materialization.asset_deps == [SILVER_EXCHANGE_SCHEDULE_ASSET]


def test_bronze_asset_metadata_classifies_changed_unchanged_and_not_applicable() -> None:
    """Domain asset helpers should attach a consistent change-kind contract."""
    changed = attach_eod_price_bronze_metadata(rows_written=4)
    unchanged = attach_eod_price_bronze_metadata(rows_written=0, reason="already_ingested")
    not_applicable = attach_exchange_holiday_bronze_metadata(rows_written=0, reason="no_holiday")
    aggregate = attach_fundamental_bronze_metadata(rows_written=0, reason="not_stock,no_facts")

    assert changed["asset_domain"] == "price"
    assert changed["change_kind"] == "changed"
    assert unchanged["change_kind"] == "unchanged"
    assert not_applicable["change_kind"] == "not_applicable"
    assert aggregate["asset_grain"] == "table_group"
    assert aggregate["change_kind"] == "not_applicable"
