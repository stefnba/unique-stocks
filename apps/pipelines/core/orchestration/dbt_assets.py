"""Generic dbt asset materialization selection helpers."""

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

type DbtAssetGroup = str
type DbtAssetLayer = Literal["silver", "gold"]

_SELECTOR_TOKEN_RE = re.compile(r"[a-z0-9_]+")


class DbtAssetRecorder(Protocol):
    """Callable that records one dbt asset group's materializations."""

    def __call__(self, *, layers: tuple[str, ...], **metadata: object) -> None:
        """Record materialization metadata."""


@dataclass(frozen=True, slots=True)
class DbtAssetSpec:
    """dbt selector and recorder metadata for one app-owned asset group."""

    group: DbtAssetGroup
    select_needles: tuple[str, ...]
    recorder: DbtAssetRecorder


@dataclass(frozen=True, slots=True)
class DbtAssetMaterialization:
    """Domain and layer materialization selected from dbt artifacts."""

    group: DbtAssetGroup
    layers: tuple[DbtAssetLayer, ...]
    models: tuple[dict[str, object], ...] = ()


def record_dbt_asset_materializations_for_specs(
    *,
    select: Sequence[str],
    metadata: dict[str, object],
    specs: Sequence[DbtAssetSpec],
) -> None:
    """Record materializations for dbt assets selected by one command."""
    specs_by_group = spec_by_group(specs)
    for materialization in selected_dbt_asset_materializations(select=select, metadata=metadata, specs=specs):
        specs_by_group[materialization.group].recorder(
            layers=tuple(materialization.layers),
            **asset_metadata_for_materialization(metadata=metadata, materialization=materialization),
        )


def selected_dbt_asset_materializations(
    *,
    select: Sequence[str],
    specs: Sequence[DbtAssetSpec],
    metadata: Mapping[str, object] | None = None,
) -> list[DbtAssetMaterialization]:
    """Return asset groups and layers selected by a dbt command."""
    models = dbt_materialized_models(metadata or {})
    if models:
        from_models = _selected_materializations_from_models(models=models, specs=specs)
        if from_models:
            return from_models

    return [
        DbtAssetMaterialization(group=spec.group, layers=selected_layers_from_select(select))
        for spec in selected_dbt_asset_specs(select=select, specs=specs)
    ]


def selected_dbt_asset_specs(*, select: Sequence[str], specs: Sequence[DbtAssetSpec]) -> list[DbtAssetSpec]:
    """Return dbt asset specs that match a dbt select expression."""
    specs = list(specs)
    if not select:
        return specs

    selector_tokens = _selector_tokens(select)
    return [spec for spec in specs if selector_tokens.intersection(needle.lower() for needle in spec.select_needles)]


def selected_layers_from_select(select: Sequence[str]) -> tuple[DbtAssetLayer, ...]:
    """Infer selected asset layers from a dbt selector string."""
    if not select:
        return ("silver", "gold")

    selector_tokens = _selector_tokens(select)
    layers: set[DbtAssetLayer] = set()
    if selector_tokens.intersection(("staging", "intermediate", "silver")):
        layers.add("silver")
    if selector_tokens.intersection(("mart", "marts", "gold")):
        layers.add("gold")
    return _ordered_layers(layers) if layers else ("silver", "gold")


def dbt_materialized_models(metadata: Mapping[str, object]) -> list[dict[str, object]]:
    """Return model metadata from a dbt asset-materialization payload."""
    models = metadata.get("dbt_materialized_models")
    if not isinstance(models, list):
        return []
    return [model for model in models if isinstance(model, dict)]


def asset_metadata_for_materialization(
    *,
    metadata: dict[str, object],
    materialization: DbtAssetMaterialization,
) -> dict[str, object]:
    """Return metadata scoped to one dbt asset materialization."""
    payload = {key: value for key, value in metadata.items() if key != "dbt_materialized_models"}
    run_model_count = payload.get("dbt_materialized_model_count")
    if isinstance(run_model_count, int):
        payload["dbt_run_materialized_model_count"] = run_model_count
    payload["dbt_asset_group"] = materialization.group
    payload["dbt_asset_layers"] = list(materialization.layers)
    if materialization.models:
        payload["dbt_asset_materialized_model_count"] = len(materialization.models)
        payload["dbt_materialized_model_count"] = len(materialization.models)
        payload["dbt_materialized_models"] = list(materialization.models)
        unique_ids = [str(unique_id) for model in materialization.models if (unique_id := model.get("unique_id"))]
        if unique_ids:
            payload["dbt_materialized_model_unique_ids"] = unique_ids
    return payload


def spec_by_group(specs: Sequence[DbtAssetSpec]) -> dict[DbtAssetGroup, DbtAssetSpec]:
    """Return specs keyed by group, rejecting duplicate group names."""
    by_group: dict[DbtAssetGroup, DbtAssetSpec] = {}
    duplicates: list[str] = []
    for spec in specs:
        if spec.group in by_group:
            duplicates.append(spec.group)
        by_group[spec.group] = spec
    if duplicates:
        names = ", ".join(sorted(set(duplicates)))
        raise ValueError(f"Duplicate dbt asset group(s): {names}")
    return by_group


def _selected_materializations_from_models(
    *,
    models: Sequence[dict[str, object]],
    specs: Sequence[DbtAssetSpec],
) -> list[DbtAssetMaterialization]:
    groups = set(spec_by_group(specs))
    selected: dict[DbtAssetGroup, dict[DbtAssetLayer, list[dict[str, object]]]] = {}
    for model in models:
        group = _dbt_asset_group_for_model(model=model, groups=groups)
        layer = _dbt_asset_layer_for_model(model)
        if group is None or layer is None:
            continue
        selected.setdefault(group, {}).setdefault(layer, []).append(model)

    materializations: list[DbtAssetMaterialization] = []
    for spec in specs:
        layers = selected.get(spec.group)
        if not layers:
            continue
        ordered_layers = _ordered_layers(set(layers))
        materializations.append(
            DbtAssetMaterialization(
                group=spec.group,
                layers=ordered_layers,
                models=tuple(model for layer in ordered_layers for model in layers[layer]),
            )
        )
    return materializations


def _ordered_layers(layers: set[DbtAssetLayer]) -> tuple[DbtAssetLayer, ...]:
    return tuple(layer for layer in ("silver", "gold") if layer in layers)


def _selector_tokens(select: Sequence[str]) -> set[str]:
    return {token for expression in select for token in _SELECTOR_TOKEN_RE.findall(expression.lower())}


def _dbt_asset_group_for_model(
    *,
    model: dict[str, object],
    groups: set[DbtAssetGroup],
) -> DbtAssetGroup | None:
    path_parts = _dbt_model_path_parts(model)
    for layer_name in ("staging", "intermediate", "marts"):
        if layer_name not in path_parts:
            continue
        index = path_parts.index(layer_name)
        if index + 1 >= len(path_parts):
            continue
        group = path_parts[index + 1]
        if group in groups:
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
