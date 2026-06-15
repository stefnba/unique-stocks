"""Prefect asset materializations for instrument ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import record_prefect_materialization


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/instrument",
        properties=AssetProperties(name="Bronze instrument"),
    ),
    by="python",
    name="record-bronze-instrument-materialization",
)
def _record_bronze_instrument_materialization(**metadata: object) -> dict[str, object]:
    return metadata


def record_instrument_bronze_materialization(**metadata: object) -> None:
    """Record a Prefect materialization for Bronze instrument rows."""
    record_prefect_materialization(
        materialization_name="bronze.instrument",
        materializer=_record_bronze_instrument_materialization,
        metadata=metadata,
    )


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


def record_instrument_dbt_materialization(**metadata: object) -> None:
    """Record Prefect materializations for dbt-built instrument assets."""
    record_prefect_materialization(
        materialization_name="dbt.instrument",
        materializer=_record_dbt_instrument_materializations,
        metadata=metadata,
    )
