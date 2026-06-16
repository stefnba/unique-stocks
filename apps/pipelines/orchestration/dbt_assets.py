"""Application dbt asset materialization registry."""

from collections.abc import Mapping, Sequence

from core.orchestration.dbt_assets import (
    DbtAssetMaterialization,
    DbtAssetSpec,
    record_dbt_asset_materializations_for_specs,
    selected_dbt_asset_specs,
)
from core.orchestration.dbt_assets import (
    selected_dbt_asset_materializations as _selected_dbt_asset_materializations,
)
from domains.eod_price.assets import record_price_dbt_materialization
from domains.exchange.assets import record_exchange_dbt_materialization
from domains.exchange_schedule.assets import record_exchange_schedule_dbt_materialization
from domains.fundamental.assets import record_fundamental_dbt_materialization
from domains.instrument.assets import record_instrument_dbt_materialization

DBT_ASSET_SPECS: tuple[DbtAssetSpec, ...] = (
    DbtAssetSpec(
        group="exchange",
        select_needles=("exchange", "provider_namespace_policy", "eodhd_provider_namespaces"),
        recorder=record_exchange_dbt_materialization,
    ),
    DbtAssetSpec(
        group="exchange_schedule",
        select_needles=("exchange_schedule", "schedule", "holiday"),
        recorder=record_exchange_schedule_dbt_materialization,
    ),
    DbtAssetSpec(
        group="instrument",
        select_needles=("instrument",),
        recorder=record_instrument_dbt_materialization,
    ),
    DbtAssetSpec(
        group="price",
        select_needles=("price", "eod", "eod_price", "stg_eod_price", "fct_daily_price"),
        recorder=record_price_dbt_materialization,
    ),
    DbtAssetSpec(
        group="fundamental",
        select_needles=("fundamental",),
        recorder=record_fundamental_dbt_materialization,
    ),
)


def record_dbt_asset_materializations(
    *,
    select: Sequence[str],
    metadata: dict[str, object],
) -> None:
    """Record Prefect materializations for Silver/Gold dbt assets just built."""
    record_dbt_asset_materializations_for_specs(select=select, metadata=metadata, specs=DBT_ASSET_SPECS)


def selected_dbt_asset_materializations(
    *,
    select: Sequence[str],
    metadata: Mapping[str, object] | None = None,
) -> list[DbtAssetMaterialization]:
    """Return app asset groups and layers selected by a dbt command."""
    return _selected_dbt_asset_materializations(select=select, metadata=metadata, specs=DBT_ASSET_SPECS)


def selected_dbt_asset_groups(select: Sequence[str]) -> list[str]:
    """Return app asset groups that match a dbt select expression."""
    return [spec.group for spec in selected_dbt_asset_specs(select=select, specs=DBT_ASSET_SPECS)]


__all__ = [
    "DBT_ASSET_SPECS",
    "DbtAssetMaterialization",
    "record_dbt_asset_materializations",
    "selected_dbt_asset_groups",
    "selected_dbt_asset_materializations",
]
