"""Prefect asset materializations for exchange reference ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import attach_materialization_metadata, record_prefect_materialization

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


def attach_exchange_catalog_bronze_metadata(**metadata: object) -> dict[str, object]:
    """Attach metadata to the Bronze exchange catalog materialization event."""
    return attach_materialization_metadata(
        BRONZE_EXCHANGE_CATALOG_ASSET,
        {"asset_layer": "bronze", "asset_domain": "exchange", "asset_grain": "table", **metadata},
    )


def attach_exchange_mic_registry_bronze_metadata(**metadata: object) -> dict[str, object]:
    """Attach metadata to the Bronze MIC registry materialization event."""
    return attach_materialization_metadata(
        BRONZE_EXCHANGE_MIC_REGISTRY_ASSET,
        {"asset_layer": "bronze", "asset_domain": "exchange", "asset_grain": "table", **metadata},
    )


@materialize(
    BRONZE_EXCHANGE_CATALOG_ASSET,
    by="python",
    name="record-bronze-exchange-catalog-materialization",
)
def _record_bronze_exchange_catalog_materialization(**metadata: object) -> dict[str, object]:
    return attach_exchange_catalog_bronze_metadata(**metadata)


@materialize(
    BRONZE_EXCHANGE_MIC_REGISTRY_ASSET,
    by="python",
    name="record-bronze-exchange-mic-registry-materialization",
)
def _record_bronze_exchange_mic_registry_materialization(
    **metadata: object,
) -> dict[str, object]:
    return attach_exchange_mic_registry_bronze_metadata(**metadata)


def record_exchange_catalog_bronze_materialization(**metadata: object) -> None:
    """Record a Prefect materialization for Bronze exchange catalog rows."""
    record_prefect_materialization(
        materialization_name="bronze.exchange_catalog",
        materializer=_record_bronze_exchange_catalog_materialization,
        metadata=metadata,
    )


def record_exchange_mic_registry_bronze_materialization(**metadata: object) -> None:
    """Record a Prefect materialization for Bronze MIC registry rows."""
    record_prefect_materialization(
        materialization_name="bronze.exchange_mic_registry",
        materializer=_record_bronze_exchange_mic_registry_materialization,
        metadata=metadata,
    )


@materialize(
    SILVER_EXCHANGE_ASSET,
    by="dbt",
    asset_deps=[BRONZE_EXCHANGE_CATALOG_ASSET, BRONZE_EXCHANGE_MIC_REGISTRY_ASSET],
    name="record-dbt-silver-exchange-materialization",
)
def _record_silver_exchange_materialization(**metadata: object) -> dict[str, object]:
    return attach_materialization_metadata(
        SILVER_EXCHANGE_ASSET,
        {"asset_layer": "silver", "asset_domain": "exchange", "asset_grain": "model_group", **metadata},
    )


@materialize(
    GOLD_EXCHANGE_ASSET,
    by="dbt",
    asset_deps=[SILVER_EXCHANGE_ASSET],
    name="record-dbt-gold-exchange-materialization",
)
def _record_gold_exchange_materialization(**metadata: object) -> dict[str, object]:
    return attach_materialization_metadata(
        GOLD_EXCHANGE_ASSET,
        {"asset_layer": "gold", "asset_domain": "exchange", "asset_grain": "model_group", **metadata},
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
