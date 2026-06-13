"""Prefect asset materializations for exchange reference ingestion."""

from prefect.assets import Asset, AssetProperties, materialize

from core.prefect.assets import record_prefect_materialization


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/exchange_catalog",
        properties=AssetProperties(name="Bronze exchange catalog"),
    ),
    by="python",
    name="record-bronze-exchange-catalog-materialization",
)
def _record_bronze_exchange_catalog_materialization(**metadata: object) -> dict[str, object]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/exchange_mic_registry",
        properties=AssetProperties(name="Bronze exchange MIC registry"),
    ),
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
