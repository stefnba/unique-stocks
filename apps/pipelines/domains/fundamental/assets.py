"""Prefect asset materializations for fundamental ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import attach_materialization_metadata, record_prefect_materialization

BRONZE_FUNDAMENTAL_ASSET = Asset(
    key="duckdb://unique-stocks/bronze/fundamental",
    properties=AssetProperties(
        name="Bronze fundamental",
        description="Aggregate materialization for bronze.fundamental_* tables.",
    ),
)
SILVER_FUNDAMENTAL_ASSET = Asset(
    key="duckdb://unique-stocks/silver/fundamental",
    properties=AssetProperties(name="Silver fundamental"),
)
GOLD_FUNDAMENTAL_ASSET = Asset(
    key="duckdb://unique-stocks/gold/fundamental",
    properties=AssetProperties(name="Gold fundamental"),
)


def attach_fundamental_bronze_metadata(**metadata: object) -> dict[str, object]:
    """Attach metadata to the aggregate Bronze fundamental materialization event."""
    return attach_materialization_metadata(
        BRONZE_FUNDAMENTAL_ASSET,
        {"asset_layer": "bronze", "asset_domain": "fundamental", "asset_grain": "table_group", **metadata},
    )


@materialize(
    BRONZE_FUNDAMENTAL_ASSET,
    by="python",
    name="record-bronze-fundamental-materialization",
)
def _record_bronze_fundamental_materialization(**metadata: object) -> dict[str, object]:
    return attach_fundamental_bronze_metadata(**metadata)


def record_fundamental_bronze_materialization(**metadata: object) -> None:
    """Record a Prefect materialization for Bronze fundamental rows."""
    record_prefect_materialization(
        materialization_name="bronze.fundamental",
        materializer=_record_bronze_fundamental_materialization,
        metadata=metadata,
    )


@materialize(
    SILVER_FUNDAMENTAL_ASSET,
    by="dbt",
    asset_deps=[BRONZE_FUNDAMENTAL_ASSET],
    name="record-dbt-silver-fundamental-materialization",
)
def _record_silver_fundamental_materialization(**metadata: object) -> dict[str, object]:
    return attach_materialization_metadata(
        SILVER_FUNDAMENTAL_ASSET,
        {"asset_layer": "silver", "asset_domain": "fundamental", "asset_grain": "model_group", **metadata},
    )


@materialize(
    GOLD_FUNDAMENTAL_ASSET,
    by="dbt",
    asset_deps=[SILVER_FUNDAMENTAL_ASSET],
    name="record-dbt-gold-fundamental-materialization",
)
def _record_gold_fundamental_materialization(**metadata: object) -> dict[str, object]:
    return attach_materialization_metadata(
        GOLD_FUNDAMENTAL_ASSET,
        {"asset_layer": "gold", "asset_domain": "fundamental", "asset_grain": "model_group", **metadata},
    )


def record_fundamental_dbt_materialization(*, layers: tuple[str, ...] = ("silver", "gold"), **metadata: object) -> None:
    """Record Prefect materializations for dbt-built fundamental assets."""
    if "silver" in layers:
        record_prefect_materialization(
            materialization_name="dbt.fundamental.silver",
            materializer=_record_silver_fundamental_materialization,
            metadata=metadata,
        )
    if "gold" in layers:
        record_prefect_materialization(
            materialization_name="dbt.fundamental.gold",
            materializer=_record_gold_fundamental_materialization,
            metadata=metadata,
        )
