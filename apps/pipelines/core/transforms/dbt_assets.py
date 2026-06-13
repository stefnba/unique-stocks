"""Prefect asset materializations for dbt-built transformation layers."""

from collections.abc import Sequence

from prefect.assets import Asset, AssetProperties, materialize

from core.prefect.assets import record_prefect_materialization


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/exchange",
        properties=AssetProperties(name="Silver exchange"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/exchange",
        properties=AssetProperties(name="Gold exchange"),
    ),
    by="dbt",
    name="record-dbt-exchange-materializations",
)
def _record_dbt_exchange_materializations(**metadata: object) -> dict[str, object]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/exchange_schedule",
        properties=AssetProperties(name="Silver exchange schedule"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/exchange_schedule",
        properties=AssetProperties(name="Gold exchange schedule"),
    ),
    by="dbt",
    name="record-dbt-exchange-schedule-materializations",
)
def _record_dbt_exchange_schedule_materializations(**metadata: object) -> dict[str, object]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/instrument",
        properties=AssetProperties(name="Silver instrument"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/instrument",
        properties=AssetProperties(name="Gold instrument"),
    ),
    by="dbt",
    name="record-dbt-instrument-materializations",
)
def _record_dbt_instrument_materializations(**metadata: object) -> dict[str, object]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/price",
        properties=AssetProperties(name="Silver price"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/price",
        properties=AssetProperties(name="Gold price"),
    ),
    by="dbt",
    name="record-dbt-price-materializations",
)
def _record_dbt_price_materializations(**metadata: object) -> dict[str, object]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/fundamental",
        properties=AssetProperties(name="Silver fundamental"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/fundamental",
        properties=AssetProperties(name="Gold fundamental"),
    ),
    by="dbt",
    name="record-dbt-fundamental-materializations",
)
def _record_dbt_fundamental_materializations(**metadata: object) -> dict[str, object]:
    return metadata


def record_dbt_asset_materializations(
    *,
    select: Sequence[str],
    metadata: dict[str, object],
) -> None:
    """Record Prefect materializations for Silver/Gold dbt assets just built."""
    materializers = {
        "exchange": _record_dbt_exchange_materializations,
        "exchange_schedule": _record_dbt_exchange_schedule_materializations,
        "instrument": _record_dbt_instrument_materializations,
        "price": _record_dbt_price_materializations,
        "fundamental": _record_dbt_fundamental_materializations,
    }
    for group in selected_dbt_asset_groups(select):
        record_prefect_materialization(
            materialization_name=f"dbt.{group}",
            materializer=materializers[group],
            metadata=metadata,
        )


def selected_dbt_asset_groups(select: Sequence[str]) -> list[str]:
    """Return dbt asset groups that match a dbt select expression."""
    if not select:
        return ["exchange", "exchange_schedule", "instrument", "price", "fundamental"]

    joined = " ".join(select).lower()
    groups: list[str] = []
    for group, needles in {
        "exchange": ("exchange", "provider_namespace"),
        "exchange_schedule": ("exchange_schedule", "schedule", "holiday"),
        "instrument": ("instrument",),
        "price": ("price", "eod"),
        "fundamental": ("fundamental",),
    }.items():
        if any(needle in joined for needle in needles):
            groups.append(group)
    return groups or ["exchange", "exchange_schedule", "instrument", "price", "fundamental"]
