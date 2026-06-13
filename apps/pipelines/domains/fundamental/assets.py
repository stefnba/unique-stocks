"""Prefect asset materializations for fundamental ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.prefect.assets import record_prefect_materialization


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/fundamental",
        properties=AssetProperties(
            name="Bronze fundamental",
            description="Aggregate materialization for bronze.fundamental_* tables.",
        ),
    ),
    by="python",
    name="record-bronze-fundamental-materialization",
)
def _record_bronze_fundamental_materialization(**metadata: object) -> dict[str, object]:
    return metadata


def record_fundamental_bronze_materialization(**metadata: object) -> None:
    """Record a Prefect materialization for Bronze fundamental rows."""
    record_prefect_materialization(
        materialization_name="bronze.fundamental",
        materializer=_record_bronze_fundamental_materialization,
        metadata=metadata,
    )
