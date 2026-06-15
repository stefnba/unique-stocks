"""Prefect asset materializations for instrument ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import record_prefect_materialization

BRONZE_INSTRUMENT_ASSET = Asset(
    key="duckdb://unique-stocks/bronze/instrument",
    properties=AssetProperties(name="Bronze instrument"),
)
SILVER_INSTRUMENT_ASSET = Asset(
    key="duckdb://unique-stocks/silver/instrument",
    properties=AssetProperties(name="Silver instrument"),
)
GOLD_INSTRUMENT_ASSET = Asset(
    key="duckdb://unique-stocks/gold/instrument",
    properties=AssetProperties(name="Gold instrument"),
)


@materialize(
    BRONZE_INSTRUMENT_ASSET,
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
    SILVER_INSTRUMENT_ASSET,
    GOLD_INSTRUMENT_ASSET,
    by="dbt",
    name="record-dbt-instrument-materializations",
    asset_deps=[BRONZE_INSTRUMENT_ASSET],
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
