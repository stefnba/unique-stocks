"""Prefect asset materializations for instrument ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.prefect.assets import record_prefect_materialization


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
