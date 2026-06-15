"""Prefect asset materializations for EOD price ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import attach_materialization_metadata, record_prefect_materialization

BRONZE_EOD_PRICE_ASSET = Asset(
    key="duckdb://unique-stocks/bronze/eod_price",
    properties=AssetProperties(
        name="Bronze EOD price",
        description="Provider-validated EOD price rows written by Python ingestion.",
    ),
)
SILVER_PRICE_ASSET = Asset(
    key="duckdb://unique-stocks/silver/price",
    properties=AssetProperties(name="Silver price"),
)
GOLD_PRICE_ASSET = Asset(
    key="duckdb://unique-stocks/gold/price",
    properties=AssetProperties(name="Gold price"),
)


def attach_eod_price_bronze_metadata(**metadata: object) -> dict[str, object]:
    """Attach metadata to the Bronze EOD price materialization event."""
    return attach_materialization_metadata(
        BRONZE_EOD_PRICE_ASSET,
        {"asset_layer": "bronze", "asset_domain": "price", "asset_grain": "table", **metadata},
    )


@materialize(
    BRONZE_EOD_PRICE_ASSET,
    by="python",
    name="record-bronze-eod-price-materialization",
)
def _record_bronze_eod_price_materialization(**metadata: object) -> dict[str, object]:
    return attach_eod_price_bronze_metadata(**metadata)


def record_eod_price_bronze_materialization(**metadata: object) -> None:
    """Record a Prefect materialization for Bronze EOD price rows already written."""
    record_prefect_materialization(
        materialization_name="bronze.eod_price",
        materializer=_record_bronze_eod_price_materialization,
        metadata=metadata,
    )


@materialize(
    SILVER_PRICE_ASSET,
    by="dbt",
    asset_deps=[BRONZE_EOD_PRICE_ASSET],
    name="record-dbt-silver-price-materialization",
)
def _record_silver_price_materialization(**metadata: object) -> dict[str, object]:
    return attach_materialization_metadata(
        SILVER_PRICE_ASSET,
        {"asset_layer": "silver", "asset_domain": "price", "asset_grain": "model_group", **metadata},
    )


@materialize(
    GOLD_PRICE_ASSET,
    by="dbt",
    asset_deps=[SILVER_PRICE_ASSET],
    name="record-dbt-gold-price-materialization",
)
def _record_gold_price_materialization(**metadata: object) -> dict[str, object]:
    return attach_materialization_metadata(
        GOLD_PRICE_ASSET,
        {"asset_layer": "gold", "asset_domain": "price", "asset_grain": "model_group", **metadata},
    )


def record_price_dbt_materialization(*, layers: tuple[str, ...] = ("silver", "gold"), **metadata: object) -> None:
    """Record Prefect materializations for dbt-built price assets."""
    if "silver" in layers:
        record_prefect_materialization(
            materialization_name="dbt.price.silver",
            materializer=_record_silver_price_materialization,
            metadata=metadata,
        )
    if "gold" in layers:
        record_prefect_materialization(
            materialization_name="dbt.price.gold",
            materializer=_record_gold_price_materialization,
            metadata=metadata,
        )
