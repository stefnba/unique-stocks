"""Application dbt asset materialization registry.

This module maps dbt selectors to the concrete domain asset materializers in
``domains.*.assets``. It is intentionally outside ``core`` because the asset
groups, selector needles, and materializer functions are app-specific.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

from domains.eod_price.assets import record_price_dbt_materialization
from domains.exchange.assets import record_exchange_dbt_materialization
from domains.exchange_schedule.assets import record_exchange_schedule_dbt_materialization
from domains.fundamental.assets import record_fundamental_dbt_materialization
from domains.instrument.assets import record_instrument_dbt_materialization
from orchestration.domain_dbt import DOMAIN_DBT_SPECS

type DbtAssetGroup = str
type DbtAssetLayer = Literal["silver", "gold"]


@dataclass(frozen=True, slots=True)
class DbtAssetMaterialization:
    """Domain and layer materialization selected from dbt artifacts."""

    group: DbtAssetGroup
    layers: tuple[DbtAssetLayer, ...]


class DbtAssetRecorder(Protocol):
    """Callable that records one domain's dbt materializations."""

    def __call__(self, *, layers: tuple[str, ...], **metadata: object) -> None:
        """Record materialization metadata."""


DBT_ASSET_GROUPS: tuple[DbtAssetGroup, ...] = tuple(spec.asset_group for spec in DOMAIN_DBT_SPECS)

_DBT_ASSET_RECORDERS: dict[DbtAssetGroup, DbtAssetRecorder] = {
    "exchange": record_exchange_dbt_materialization,
    "exchange_schedule": record_exchange_schedule_dbt_materialization,
    "instrument": record_instrument_dbt_materialization,
    "price": record_price_dbt_materialization,
    "fundamental": record_fundamental_dbt_materialization,
}

_DBT_ASSET_SELECT_NEEDLES: dict[DbtAssetGroup, tuple[str, ...]] = {
    spec.asset_group: spec.select_needles for spec in DOMAIN_DBT_SPECS
}


def _validate_dbt_asset_registry() -> None:
    """Fail fast if registry dbt groups and recorder implementations drift."""
    configured = set(DBT_ASSET_GROUPS)
    implemented = set(_DBT_ASSET_RECORDERS)
    missing = sorted(configured - implemented)
    extra = sorted(implemented - configured)
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing recorders for: {', '.join(missing)}")
        if extra:
            details.append(f"unconfigured recorders for: {', '.join(extra)}")
        msg = "Dbt asset registry does not match domain registry: " + "; ".join(details)
        raise RuntimeError(msg)


_validate_dbt_asset_registry()


def record_dbt_asset_materializations(
    *,
    select: Sequence[str],
    metadata: dict[str, object],
) -> None:
    """Record Prefect materializations for Silver/Gold dbt assets just built."""
    models = _dbt_materialized_models(metadata)
    for materialization in selected_dbt_asset_materializations(select=select, metadata=metadata):
        model_metadata = _models_for_materialization(models=models, materialization=materialization)
        _DBT_ASSET_RECORDERS[materialization.group](
            layers=tuple(materialization.layers),
            **_asset_metadata_for_materialization(
                metadata=metadata,
                materialization=materialization,
                models=model_metadata,
            ),
        )


def selected_dbt_asset_materializations(
    *,
    select: Sequence[str],
    metadata: Mapping[str, object] | None = None,
) -> list[DbtAssetMaterialization]:
    """Return dbt asset groups and layers selected by a dbt command."""
    models = _dbt_materialized_models(metadata or {})
    if models:
        from_models = _selected_materializations_from_models(models)
        if from_models:
            return from_models

    return [
        DbtAssetMaterialization(group=group, layers=_selected_layers_from_select(select))
        for group in selected_dbt_asset_groups(select)
    ]


def selected_dbt_asset_groups(select: Sequence[str]) -> list[DbtAssetGroup]:
    """Return dbt asset groups that match a dbt select expression."""
    if not select:
        return list(DBT_ASSET_GROUPS)

    joined = " ".join(select).lower()
    groups: list[DbtAssetGroup] = []
    for group, needles in _DBT_ASSET_SELECT_NEEDLES.items():
        if any(needle in joined for needle in needles):
            groups.append(group)
    return groups


def _selected_materializations_from_models(models: Sequence[dict[str, object]]) -> list[DbtAssetMaterialization]:
    selected: dict[DbtAssetGroup, set[DbtAssetLayer]] = {}
    for model in models:
        group = _dbt_asset_group_for_model(model)
        layer = _dbt_asset_layer_for_model(model)
        if group is None or layer is None:
            continue
        selected.setdefault(group, set()).add(layer)

    materializations: list[DbtAssetMaterialization] = []
    for group in DBT_ASSET_GROUPS:
        layers = selected.get(group)
        if layers:
            materializations.append(DbtAssetMaterialization(group=group, layers=_ordered_layers(layers)))
    return materializations


def _selected_layers_from_select(select: Sequence[str]) -> tuple[DbtAssetLayer, ...]:
    if not select:
        return ("silver", "gold")

    joined = " ".join(select).lower()
    layers: set[DbtAssetLayer] = set()
    if any(needle in joined for needle in ("staging", "intermediate", "silver")):
        layers.add("silver")
    if any(needle in joined for needle in ("mart", "marts", "gold")):
        layers.add("gold")
    return _ordered_layers(layers) if layers else ("silver", "gold")


def _ordered_layers(layers: set[DbtAssetLayer]) -> tuple[DbtAssetLayer, ...]:
    return tuple(layer for layer in ("silver", "gold") if layer in layers)


def _dbt_materialized_models(metadata: Mapping[str, object]) -> list[dict[str, object]]:
    models = metadata.get("dbt_materialized_models")
    if not isinstance(models, list):
        return []
    return [model for model in models if isinstance(model, dict)]


def _models_for_materialization(
    *,
    models: Sequence[dict[str, object]],
    materialization: DbtAssetMaterialization,
) -> list[dict[str, object]]:
    return [
        model
        for model in models
        if _dbt_asset_group_for_model(model) == materialization.group
        and _dbt_asset_layer_for_model(model) in materialization.layers
    ]


def _asset_metadata_for_materialization(
    *,
    metadata: dict[str, object],
    materialization: DbtAssetMaterialization,
    models: Sequence[dict[str, object]],
) -> dict[str, object]:
    payload = {key: value for key, value in metadata.items() if key != "dbt_materialized_models"}
    run_model_count = payload.get("dbt_materialized_model_count")
    if isinstance(run_model_count, int):
        payload["dbt_run_materialized_model_count"] = run_model_count
    payload["dbt_asset_group"] = materialization.group
    payload["dbt_asset_layers"] = list(materialization.layers)
    if models:
        payload["dbt_asset_materialized_model_count"] = len(models)
        payload["dbt_materialized_model_count"] = len(models)
        payload["dbt_materialized_models"] = list(models)
        payload["dbt_materialized_model_unique_ids"] = [str(model["unique_id"]) for model in models]
    return payload


def _dbt_asset_group_for_model(model: dict[str, object]) -> DbtAssetGroup | None:
    path_parts = _dbt_model_path_parts(model)
    for layer_name in ("staging", "intermediate", "marts"):
        if layer_name not in path_parts:
            continue
        index = path_parts.index(layer_name)
        if index + 1 >= len(path_parts):
            continue
        group = path_parts[index + 1]
        if group in DBT_ASSET_GROUPS:
            return group
    return None


def _dbt_asset_layer_for_model(model: dict[str, object]) -> DbtAssetLayer | None:
    path_parts = _dbt_model_path_parts(model)
    if "marts" in path_parts:
        return "gold"
    if "staging" in path_parts or "intermediate" in path_parts:
        return "silver"
    return None


def _dbt_model_path_parts(model: dict[str, object]) -> list[str]:
    path = model.get("original_file_path") or model.get("path") or ""
    return [part for part in str(path).replace("\\", "/").split("/") if part]
