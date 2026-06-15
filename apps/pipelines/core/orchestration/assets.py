"""Generic Prefect asset materialization helpers."""

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from prefect.assets import Asset, add_asset_metadata
from prefect.context import get_run_context
from structlog.typing import FilteringBoundLogger

logger: FilteringBoundLogger = structlog.get_logger(__name__)

type PrefectAssetRef = Asset | str


def record_prefect_materialization(
    *,
    materialization_name: str,
    materializer: Callable[..., object],
    metadata: dict[str, object],
) -> None:
    """Record one Prefect asset materialization without failing caller work."""
    try:
        get_run_context()
    except RuntimeError:
        logger.debug(
            "prefect_asset_materialization_skipped",
            materialization_name=materialization_name,
            reason="missing_run_context",
        )
        return

    try:
        materializer(**metadata)
    except Exception as exc:
        logger.warning(
            "prefect_asset_materialization_failed",
            materialization_name=materialization_name,
            error=str(exc),
        )


def attach_materialization_metadata(
    asset: PrefectAssetRef,
    metadata: Mapping[str, object],
) -> dict[str, object]:
    """Attach JSON-friendly metadata to one asset materialization event."""
    payload = jsonable_metadata(metadata)
    _add_asset_metadata_if_available(asset, payload)
    return payload


def attach_materializations_metadata(
    assets: Sequence[PrefectAssetRef],
    metadata: Mapping[str, object],
) -> dict[str, object]:
    """Attach the same metadata to several asset materialization events."""
    payload = jsonable_metadata(metadata)
    for asset in assets:
        _add_asset_metadata_if_available(asset, payload)
    return payload


def jsonable_metadata(metadata: Mapping[str, object]) -> dict[str, object]:
    """Return metadata shaped for Prefect event payloads and UI display."""
    return {str(key): _jsonable_metadata_value(value) for key, value in metadata.items()}


def _add_asset_metadata_if_available(asset: PrefectAssetRef, metadata: dict[str, object]) -> None:
    """Attach asset metadata when Prefect has an active asset context."""
    try:
        add_asset_metadata(asset, metadata)
    except RuntimeError as exc:
        if "AssetContext" not in str(exc):
            raise
        logger.debug(
            "prefect_asset_metadata_skipped",
            reason="missing_asset_context",
            asset=str(asset),
        )


def _jsonable_metadata_value(value: object) -> Any:
    """Convert common Python values to Prefect/JSON-friendly metadata values."""
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json")
    if isinstance(value, Mapping):
        return {str(key): _jsonable_metadata_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray | memoryview):
        return [_jsonable_metadata_value(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date | datetime | UUID):
        return str(value)
    return value
