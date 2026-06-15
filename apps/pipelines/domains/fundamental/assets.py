"""Prefect asset materializations for fundamental ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import record_prefect_materialization


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


def record_fundamental_dbt_materialization(**metadata: object) -> None:
    """Record Prefect materializations for dbt-built fundamental assets."""
    record_prefect_materialization(
        materialization_name="dbt.fundamental",
        materializer=_record_dbt_fundamental_materializations,
        metadata=metadata,
    )
