"""Prefect asset materializations for exchange reference ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.orchestration.assets import record_prefect_materialization

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


@materialize(
    BRONZE_EXCHANGE_CATALOG_ASSET,
    by="python",
    name="record-bronze-exchange-catalog-materialization",
)
def _record_bronze_exchange_catalog_materialization(**metadata: object) -> dict[str, object]:
    return metadata


@materialize(
    BRONZE_EXCHANGE_MIC_REGISTRY_ASSET,
    by="python",
    name="record-bronze-exchange-mic-registry-materialization",
)
def _record_bronze_exchange_mic_registry_materialization(
    **metadata: object,
) -> dict[str, object]:
    return metadata


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
    GOLD_EXCHANGE_ASSET,
    by="dbt",
    name="record-dbt-exchange-materializations",
    asset_deps=[BRONZE_EXCHANGE_CATALOG_ASSET, BRONZE_EXCHANGE_MIC_REGISTRY_ASSET],
)
def _record_dbt_exchange_materializations(**metadata: object) -> dict[str, object]:
    return metadata


def record_exchange_dbt_materialization(**metadata: object) -> None:
    """Record Prefect materializations for dbt-built exchange assets."""
    record_prefect_materialization(
        materialization_name="dbt.exchange",
        materializer=_record_dbt_exchange_materializations,
        metadata=metadata,
    )
