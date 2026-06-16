"""Generic Prefect asset materialization helpers."""

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

import structlog
from prefect.assets import Asset, add_asset_metadata
from prefect.context import get_run_context
from structlog.typing import FilteringBoundLogger

logger: FilteringBoundLogger = structlog.get_logger(__name__)

type PrefectAssetRef = Asset | str


class AssetChangeKind(StrEnum):
    """Materialization outcome semantics for downstream readers and automations."""

    CHANGED = "changed"
    UNCHANGED = "unchanged"
    NOT_APPLICABLE = "not_applicable"


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


def asset_materialization_metadata(
    *,
    domain: str,
    layer: str,
    grain: str,
    change_kind: object | None = None,
    not_applicable_reasons: object = (),
    **metadata: object,
) -> dict[str, object]:
    """Build standardized JSON-friendly metadata for one asset materialization."""
    payload = {
        "asset_domain": domain,
        "asset_layer": layer,
        "asset_grain": grain,
        **metadata,
    }
    payload["change_kind"] = str(
        change_kind
        or payload.get("change_kind")
        or infer_asset_change_kind(
            rows_written=_materialized_count(payload),
            reason=payload.get("reason"),
            not_applicable_reasons=tuple(_reason_tokens(not_applicable_reasons)),
        )
    )
    return jsonable_metadata(payload)


def attach_asset_materialization_metadata(
    asset: PrefectAssetRef,
    *,
    domain: str,
    layer: str,
    grain: str,
    change_kind: object | None = None,
    not_applicable_reasons: object = (),
    **metadata: object,
) -> dict[str, object]:
    """Build and attach standardized metadata to one asset materialization."""
    return attach_materialization_metadata(
        asset,
        asset_materialization_metadata(
            domain=domain,
            layer=layer,
            grain=grain,
            change_kind=change_kind,
            not_applicable_reasons=not_applicable_reasons,
            **metadata,
        ),
    )


def infer_asset_change_kind(
    *,
    rows_written: object | None = None,
    reason: object | None = None,
    not_applicable_reasons: Sequence[str] = (),
) -> AssetChangeKind:
    """Classify whether a materialization represents changed data."""
    count = _coerce_count(rows_written)
    reasons = _reason_tokens(reason)
    not_applicable = set(not_applicable_reasons)

    if count is not None:
        if count > 0:
            return AssetChangeKind.CHANGED
        if reasons and reasons <= not_applicable:
            return AssetChangeKind.NOT_APPLICABLE
        return AssetChangeKind.UNCHANGED

    if reasons:
        if reasons <= not_applicable:
            return AssetChangeKind.NOT_APPLICABLE
        return AssetChangeKind.UNCHANGED

    return AssetChangeKind.CHANGED


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


def _materialized_count(metadata: Mapping[str, object]) -> object | None:
    for key in (
        "rows_written",
        "dbt_asset_materialized_model_count",
        "dbt_materialized_model_count",
        "dbt_run_materialized_model_count",
    ):
        if metadata.get(key) is not None:
            return metadata[key]
    return None


def _coerce_count(value: object | None) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value)
    try:
        return int(str(value))
    except TypeError, ValueError:
        return None


def _reason_tokens(reason: object | None) -> set[str]:
    if reason is None:
        return set()
    if isinstance(reason, str):
        return {token.strip() for token in reason.split(",") if token.strip()}
    if isinstance(reason, Sequence) and not isinstance(reason, bytes | bytearray | memoryview):
        return {str(token).strip() for token in reason if str(token).strip()}
    return {str(reason).strip()} if str(reason).strip() else set()


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
