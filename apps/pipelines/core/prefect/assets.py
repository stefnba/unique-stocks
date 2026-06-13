"""Prefect asset materialization helpers for lake and dbt outputs."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import structlog
from prefect.assets import Asset, AssetProperties, materialize

logger = structlog.get_logger(__name__)


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
def _record_bronze_eod_price_materialization(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/exchange_catalog",
        properties=AssetProperties(name="Bronze exchange catalog"),
    ),
    by="python",
    name="record-bronze-exchange-catalog-materialization",
)
def _record_bronze_exchange_catalog_materialization(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/exchange_mic_registry",
        properties=AssetProperties(name="Bronze exchange MIC registry"),
    ),
    by="python",
    name="record-bronze-exchange-mic-registry-materialization",
)
def _record_bronze_exchange_mic_registry_materialization(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/exchange_schedule",
        properties=AssetProperties(name="Bronze exchange schedule"),
    ),
    by="python",
    name="record-bronze-exchange-schedule-materialization",
)
def _record_bronze_exchange_schedule_materialization(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/exchange_holiday",
        properties=AssetProperties(name="Bronze exchange holiday"),
    ),
    by="python",
    name="record-bronze-exchange-holiday-materialization",
)
def _record_bronze_exchange_holiday_materialization(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/instrument",
        properties=AssetProperties(name="Bronze instrument"),
    ),
    by="python",
    name="record-bronze-instrument-materialization",
)
def _record_bronze_instrument_materialization(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/fundamental",
        properties=AssetProperties(
            name="Bronze fundamental",
            description="Aggregate materialization for bronze.fundamental_* tables.",
        ),
    ),
    by="python",
    name="record-bronze-fundamental-materialization",
)
def _record_bronze_fundamental_materialization(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/exchange",
        properties=AssetProperties(name="Silver exchange"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/exchange",
        properties=AssetProperties(name="Gold exchange"),
    ),
    by="dbt",
    name="record-dbt-exchange-materializations",
)
def _record_dbt_exchange_materializations(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/exchange_schedule",
        properties=AssetProperties(name="Silver exchange schedule"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/exchange_schedule",
        properties=AssetProperties(name="Gold exchange schedule"),
    ),
    by="dbt",
    name="record-dbt-exchange-schedule-materializations",
)
def _record_dbt_exchange_schedule_materializations(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/instrument",
        properties=AssetProperties(name="Silver instrument"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/instrument",
        properties=AssetProperties(name="Gold instrument"),
    ),
    by="dbt",
    name="record-dbt-instrument-materializations",
)
def _record_dbt_instrument_materializations(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/price",
        properties=AssetProperties(name="Silver price"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/price",
        properties=AssetProperties(name="Gold price"),
    ),
    by="dbt",
    name="record-dbt-price-materializations",
)
def _record_dbt_price_materializations(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/fundamental",
        properties=AssetProperties(name="Silver fundamental"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/fundamental",
        properties=AssetProperties(name="Gold fundamental"),
    ),
    by="dbt",
    name="record-dbt-fundamental-materializations",
)
def _record_dbt_fundamental_materializations(**metadata: Any) -> dict[str, Any]:
    return metadata


def record_prefect_bronze_eod_price_materialization(**metadata: Any) -> None:
    """Record a Prefect asset materialization for Bronze EOD price writes.

    This does not write lake data. It publishes metadata about rows already
    written so Prefect can show lineage and recent materializations.
    """
    record_prefect_bronze_materializations(["eod_price"], metadata=metadata)


def record_prefect_bronze_materializations(
    asset_names: Sequence[str],
    *,
    metadata: dict[str, Any],
) -> None:
    """Record Prefect materializations for one or more logical Bronze assets."""
    materializers: dict[str, Callable[..., Any]] = {
        "eod_price": _record_bronze_eod_price_materialization,
        "exchange_catalog": _record_bronze_exchange_catalog_materialization,
        "exchange_mic_registry": _record_bronze_exchange_mic_registry_materialization,
        "exchange_schedule": _record_bronze_exchange_schedule_materialization,
        "exchange_holiday": _record_bronze_exchange_holiday_materialization,
        "instrument": _record_bronze_instrument_materialization,
        "fundamental": _record_bronze_fundamental_materialization,
    }
    for asset_name in asset_names:
        materializer = materializers.get(asset_name)
        if materializer is None:
            logger.warning("prefect_unknown_bronze_asset", asset_name=asset_name)
            continue
        _record_bronze_materialization(asset_name=asset_name, materializer=materializer, metadata=metadata)


def _record_bronze_materialization(
    *,
    asset_name: str,
    materializer: Callable[..., Any],
    metadata: dict[str, Any],
) -> None:
    try:
        materializer(**metadata)
    except Exception as exc:
        logger.warning(
            "prefect_bronze_asset_materialization_failed",
            asset_name=asset_name,
            error=str(exc),
        )


def record_prefect_dbt_materializations(
    *,
    select: Sequence[str],
    metadata: dict[str, Any],
) -> None:
    """Record Prefect materializations for Silver/Gold dbt assets just built."""
    groups = _selected_dbt_asset_groups(select)
    materializers = {
        "exchange": _record_dbt_exchange_materializations,
        "exchange_schedule": _record_dbt_exchange_schedule_materializations,
        "instrument": _record_dbt_instrument_materializations,
        "price": _record_dbt_price_materializations,
        "fundamental": _record_dbt_fundamental_materializations,
    }
    for group in groups:
        try:
            materializers[group](**metadata)
        except Exception as exc:
            logger.warning(
                "prefect_dbt_asset_materialization_failed",
                asset_group=group,
                error=str(exc),
            )


def _selected_dbt_asset_groups(select: Sequence[str]) -> list[str]:
    if not select:
        return ["exchange", "exchange_schedule", "instrument", "price", "fundamental"]

    joined = " ".join(select).lower()
    groups: list[str] = []
    for group, needles in {
        "exchange": ("exchange", "provider_namespace"),
        "exchange_schedule": ("exchange_schedule", "schedule", "holiday"),
        "instrument": ("instrument",),
        "price": ("price", "eod"),
        "fundamental": ("fundamental",),
    }.items():
        if any(needle in joined for needle in needles):
            groups.append(group)
    return groups or ["exchange", "exchange_schedule", "instrument", "price", "fundamental"]
