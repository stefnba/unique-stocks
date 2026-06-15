"""Prefect asset materializations for exchange schedule ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import record_prefect_materialization

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


@materialize(
    BRONZE_EXCHANGE_SCHEDULE_ASSET,
    by="python",
    name="record-bronze-exchange-schedule-materialization",
)
def _record_bronze_exchange_schedule_materialization(
    **metadata: object,
) -> dict[str, object]:
    return metadata


@materialize(
    BRONZE_EXCHANGE_HOLIDAY_ASSET,
    by="python",
    name="record-bronze-exchange-holiday-materialization",
)
def _record_bronze_exchange_holiday_materialization(
    **metadata: object,
) -> dict[str, object]:
    return metadata


def record_exchange_schedule_bronze_materialization(**metadata: object) -> None:
    """Record a Prefect materialization for Bronze exchange schedule rows."""
    record_prefect_materialization(
        materialization_name="bronze.exchange_schedule",
        materializer=_record_bronze_exchange_schedule_materialization,
        metadata=metadata,
    )


def record_exchange_holiday_bronze_materialization(**metadata: object) -> None:
    """Record a Prefect materialization for Bronze exchange holiday rows."""
    record_prefect_materialization(
        materialization_name="bronze.exchange_holiday",
        materializer=_record_bronze_exchange_holiday_materialization,
        metadata=metadata,
    )


@materialize(
    SILVER_EXCHANGE_SCHEDULE_ASSET,
    GOLD_EXCHANGE_SCHEDULE_ASSET,
    by="dbt",
    name="record-dbt-exchange-schedule-materializations",
    asset_deps=[BRONZE_EXCHANGE_SCHEDULE_ASSET, BRONZE_EXCHANGE_HOLIDAY_ASSET],
)
def _record_dbt_exchange_schedule_materializations(**metadata: object) -> dict[str, object]:
    return metadata


def record_exchange_schedule_dbt_materialization(**metadata: object) -> None:
    """Record Prefect materializations for dbt-built exchange schedule assets."""
    record_prefect_materialization(
        materialization_name="dbt.exchange_schedule",
        materializer=_record_dbt_exchange_schedule_materializations,
        metadata=metadata,
    )
