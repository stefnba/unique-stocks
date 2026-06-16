"""Prefect asset materializations for exchange reference ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import (
    attach_asset_materialization_metadata,
    record_prefect_materialization,
)

BRONZE_EXCHANGE_CATALOG_ASSET = Asset(
    key="duckdb://unique-stocks/bronze/exchange_catalog",
    properties=AssetProperties(name="Bronze exchange catalog"),
)
BRONZE_EXCHANGE_MIC_REGISTRY_ASSET = Asset(
    key="duckdb://unique-stocks/bronze/exchange_mic_registry",
    properties=AssetProperties(name="Bronze exchange MIC registry"),
)
SILVER_EXCHANGE_ASSET = Asset(
    key="duckdb://unique-stocks/silver/exchange",
    properties=AssetProperties(name="Silver exchange"),
)
GOLD_EXCHANGE_ASSET = Asset(
    key="duckdb://unique-stocks/gold/exchange",
    properties=AssetProperties(name="Gold exchange"),
)

EXCHANGE_NOT_APPLICABLE_REASONS = ("no_data",)


def attach_exchange_catalog_bronze_metadata(**metadata: object) -> dict[str, object]:
    """Attach metadata to the Bronze exchange catalog materialization event."""
    return attach_asset_materialization_metadata(
        BRONZE_EXCHANGE_CATALOG_ASSET,
        domain="exchange",
        layer="bronze",
        grain="table",
        not_applicable_reasons=EXCHANGE_NOT_APPLICABLE_REASONS,
        **metadata,
    )


def attach_exchange_mic_registry_bronze_metadata(**metadata: object) -> dict[str, object]:
    """Attach metadata to the Bronze MIC registry materialization event."""
    return attach_asset_materialization_metadata(
        BRONZE_EXCHANGE_MIC_REGISTRY_ASSET,
        domain="exchange",
        layer="bronze",
        grain="table",
        not_applicable_reasons=EXCHANGE_NOT_APPLICABLE_REASONS,
        **metadata,
    )


@materialize(
    SILVER_EXCHANGE_ASSET,
    by="dbt",
    asset_deps=[BRONZE_EXCHANGE_CATALOG_ASSET, BRONZE_EXCHANGE_MIC_REGISTRY_ASSET],
    name="record-dbt-silver-exchange-materialization",
)
def _record_silver_exchange_materialization(**metadata: object) -> dict[str, object]:
    return attach_asset_materialization_metadata(
        SILVER_EXCHANGE_ASSET,
        domain="exchange",
        layer="silver",
        grain="model_group",
        **metadata,
    )


@materialize(
    GOLD_EXCHANGE_ASSET,
    by="dbt",
    asset_deps=[SILVER_EXCHANGE_ASSET],
    name="record-dbt-gold-exchange-materialization",
)
def _record_gold_exchange_materialization(**metadata: object) -> dict[str, object]:
    return attach_asset_materialization_metadata(
        GOLD_EXCHANGE_ASSET,
        domain="exchange",
        layer="gold",
        grain="model_group",
        **metadata,
    )


def record_exchange_dbt_materialization(*, layers: tuple[str, ...] = ("silver", "gold"), **metadata: object) -> None:
    """Record Prefect materializations for dbt-built exchange assets."""
    if "silver" in layers:
        record_prefect_materialization(
            materialization_name="dbt.exchange.silver",
            materializer=_record_silver_exchange_materialization,
            metadata=metadata,
        )
    if "gold" in layers:
        record_prefect_materialization(
            materialization_name="dbt.exchange.gold",
            materializer=_record_gold_exchange_materialization,
            metadata=metadata,
        )
