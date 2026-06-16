"""Prefect asset materializations for exchange schedule ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import (
    attach_asset_materialization_metadata,
    record_prefect_materialization,
)

BRONZE_EXCHANGE_SCHEDULE_ASSET = Asset(
    key="duckdb://unique-stocks/bronze/exchange_schedule",
    properties=AssetProperties(name="Bronze exchange schedule"),
)
BRONZE_EXCHANGE_HOLIDAY_ASSET = Asset(
    key="duckdb://unique-stocks/bronze/exchange_holiday",
    properties=AssetProperties(name="Bronze exchange holiday"),
)
SILVER_EXCHANGE_SCHEDULE_ASSET = Asset(
    key="duckdb://unique-stocks/silver/exchange_schedule",
    properties=AssetProperties(name="Silver exchange schedule"),
)
GOLD_EXCHANGE_SCHEDULE_ASSET = Asset(
    key="duckdb://unique-stocks/gold/exchange_schedule",
    properties=AssetProperties(name="Gold exchange schedule"),
)

EXCHANGE_SCHEDULE_NOT_APPLICABLE_REASONS = ("no_holiday",)


def attach_exchange_schedule_bronze_metadata(**metadata: object) -> dict[str, object]:
    """Attach metadata to the Bronze exchange schedule materialization event."""
    return attach_asset_materialization_metadata(
        BRONZE_EXCHANGE_SCHEDULE_ASSET,
        domain="exchange_schedule",
        layer="bronze",
        grain="table",
        not_applicable_reasons=EXCHANGE_SCHEDULE_NOT_APPLICABLE_REASONS,
        **metadata,
    )


def attach_exchange_holiday_bronze_metadata(**metadata: object) -> dict[str, object]:
    """Attach metadata to the Bronze exchange holiday materialization event."""
    return attach_asset_materialization_metadata(
        BRONZE_EXCHANGE_HOLIDAY_ASSET,
        domain="exchange_schedule",
        layer="bronze",
        grain="table",
        not_applicable_reasons=EXCHANGE_SCHEDULE_NOT_APPLICABLE_REASONS,
        **metadata,
    )


@materialize(
    SILVER_EXCHANGE_SCHEDULE_ASSET,
    by="dbt",
    asset_deps=[BRONZE_EXCHANGE_SCHEDULE_ASSET, BRONZE_EXCHANGE_HOLIDAY_ASSET],
    name="record-dbt-silver-exchange-schedule-materialization",
)
def _record_silver_exchange_schedule_materialization(**metadata: object) -> dict[str, object]:
    return attach_asset_materialization_metadata(
        SILVER_EXCHANGE_SCHEDULE_ASSET,
        domain="exchange_schedule",
        layer="silver",
        grain="model_group",
        **metadata,
    )


@materialize(
    GOLD_EXCHANGE_SCHEDULE_ASSET,
    by="dbt",
    asset_deps=[SILVER_EXCHANGE_SCHEDULE_ASSET],
    name="record-dbt-gold-exchange-schedule-materialization",
)
def _record_gold_exchange_schedule_materialization(**metadata: object) -> dict[str, object]:
    return attach_asset_materialization_metadata(
        GOLD_EXCHANGE_SCHEDULE_ASSET,
        domain="exchange_schedule",
        layer="gold",
        grain="model_group",
        **metadata,
    )


def record_exchange_schedule_dbt_materialization(
    *,
    layers: tuple[str, ...] = ("silver", "gold"),
    **metadata: object,
) -> None:
    """Record Prefect materializations for dbt-built exchange schedule assets."""
    if "silver" in layers:
        record_prefect_materialization(
            materialization_name="dbt.exchange_schedule.silver",
            materializer=_record_silver_exchange_schedule_materialization,
            metadata=metadata,
        )
    if "gold" in layers:
        record_prefect_materialization(
            materialization_name="dbt.exchange_schedule.gold",
            materializer=_record_gold_exchange_schedule_materialization,
            metadata=metadata,
        )
