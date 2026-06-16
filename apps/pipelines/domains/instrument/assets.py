"""Prefect asset materializations for instrument ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import (
    attach_asset_materialization_metadata,
    record_prefect_materialization,
)

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

INSTRUMENT_NOT_APPLICABLE_REASONS = ("no_data", "no_valid_rows")


def attach_instrument_bronze_metadata(**metadata: object) -> dict[str, object]:
    """Attach metadata to the Bronze instrument materialization event."""
    return attach_asset_materialization_metadata(
        BRONZE_INSTRUMENT_ASSET,
        domain="instrument",
        layer="bronze",
        grain="table",
        not_applicable_reasons=INSTRUMENT_NOT_APPLICABLE_REASONS,
        **metadata,
    )


@materialize(
    SILVER_INSTRUMENT_ASSET,
    by="dbt",
    asset_deps=[BRONZE_INSTRUMENT_ASSET],
    name="record-dbt-silver-instrument-materialization",
)
def _record_silver_instrument_materialization(**metadata: object) -> dict[str, object]:
    return attach_asset_materialization_metadata(
        SILVER_INSTRUMENT_ASSET,
        domain="instrument",
        layer="silver",
        grain="model_group",
        **metadata,
    )


@materialize(
    GOLD_INSTRUMENT_ASSET,
    by="dbt",
    asset_deps=[SILVER_INSTRUMENT_ASSET],
    name="record-dbt-gold-instrument-materialization",
)
def _record_gold_instrument_materialization(**metadata: object) -> dict[str, object]:
    return attach_asset_materialization_metadata(
        GOLD_INSTRUMENT_ASSET,
        domain="instrument",
        layer="gold",
        grain="model_group",
        **metadata,
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
