"""Prefect asset materializations for EOD price ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import record_prefect_materialization

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


@materialize(
    BRONZE_EOD_PRICE_ASSET,
    by="python",
    name="record-bronze-eod-price-materialization",
)
def _record_bronze_eod_price_materialization(**metadata: object) -> dict[str, object]:
    return metadata


def record_eod_price_bronze_materialization(**metadata: object) -> None:
    """Record a Prefect materialization for Bronze EOD price rows already written."""
    record_prefect_materialization(
        materialization_name="bronze.eod_price",
        materializer=_record_bronze_eod_price_materialization,
        metadata=metadata,
    )


@materialize(
    SILVER_PRICE_ASSET,
    GOLD_PRICE_ASSET,
    by="dbt",
    name="record-dbt-price-materializations",
    asset_deps=[BRONZE_EOD_PRICE_ASSET],
)
def _record_dbt_price_materializations(**metadata: object) -> dict[str, object]:
    return metadata


def record_price_dbt_materialization(**metadata: object) -> None:
    """Record Prefect materializations for dbt-built price assets."""
    record_prefect_materialization(
        materialization_name="dbt.price",
        materializer=_record_dbt_price_materializations,
        metadata=metadata,
    )
