"""Prefect asset materializations for exchange schedule ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.prefect.assets import record_prefect_materialization


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/exchange_schedule",
        properties=AssetProperties(name="Bronze exchange schedule"),
    ),
    by="python",
    name="record-bronze-exchange-schedule-materialization",
)
def _record_bronze_exchange_schedule_materialization(
    **metadata: object,
) -> dict[str, object]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/exchange_holiday",
        properties=AssetProperties(name="Bronze exchange holiday"),
    ),
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
