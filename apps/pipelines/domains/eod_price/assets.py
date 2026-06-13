"""Prefect asset materializations for EOD price ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.prefect.assets import record_prefect_materialization


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/eod_price",
        properties=AssetProperties(
            name="Bronze EOD price",
            description="Provider-validated EOD price rows written by Python ingestion.",
        ),
    ),
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
