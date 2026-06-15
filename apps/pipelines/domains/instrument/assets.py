"""Prefect asset materializations for instrument ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import attach_materialization_metadata, record_prefect_materialization

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


def attach_instrument_bronze_metadata(**metadata: object) -> dict[str, object]:
    """Attach metadata to the Bronze instrument materialization event."""
    return attach_materialization_metadata(
        BRONZE_INSTRUMENT_ASSET,
        {"asset_layer": "bronze", "asset_domain": "instrument", "asset_grain": "table", **metadata},
    )


@materialize(
    BRONZE_INSTRUMENT_ASSET,
    by="python",
    name="record-bronze-instrument-materialization",
)
def _record_bronze_instrument_materialization(**metadata: object) -> dict[str, object]:
    return attach_instrument_bronze_metadata(**metadata)


def record_instrument_bronze_materialization(**metadata: object) -> None:
    """Record a Prefect materialization for Bronze instrument rows."""
    record_prefect_materialization(
        materialization_name="bronze.instrument",
        materializer=_record_bronze_instrument_materialization,
        metadata=metadata,
    )


@materialize(
    SILVER_INSTRUMENT_ASSET,
    by="dbt",
    asset_deps=[BRONZE_INSTRUMENT_ASSET],
    name="record-dbt-silver-instrument-materialization",
)
def _record_silver_instrument_materialization(**metadata: object) -> dict[str, object]:
    return attach_materialization_metadata(
        SILVER_INSTRUMENT_ASSET,
        {"asset_layer": "silver", "asset_domain": "instrument", "asset_grain": "model_group", **metadata},
    )


@materialize(
    GOLD_INSTRUMENT_ASSET,
    by="dbt",
    asset_deps=[SILVER_INSTRUMENT_ASSET],
    name="record-dbt-gold-instrument-materialization",
)
def _record_gold_instrument_materialization(**metadata: object) -> dict[str, object]:
    return attach_materialization_metadata(
        GOLD_INSTRUMENT_ASSET,
        {"asset_layer": "gold", "asset_domain": "instrument", "asset_grain": "model_group", **metadata},
    )


def record_instrument_dbt_materialization(*, layers: tuple[str, ...] = ("silver", "gold"), **metadata: object) -> None:
    """Record Prefect materializations for dbt-built instrument assets."""
    if "silver" in layers:
        record_prefect_materialization(
            materialization_name="dbt.instrument.silver",
            materializer=_record_silver_instrument_materialization,
            metadata=metadata,
        )
    if "gold" in layers:
        record_prefect_materialization(
            materialization_name="dbt.instrument.gold",
            materializer=_record_gold_instrument_materialization,
            metadata=metadata,
        )
