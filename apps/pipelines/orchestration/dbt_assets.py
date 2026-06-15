"""Application dbt asset materialization registry.

This module maps dbt selectors to the concrete domain asset materializers in
``domains.*.assets``. It is intentionally outside ``core`` because the asset
groups, selector needles, and materializer functions are app-specific.
"""

from collections.abc import Sequence
from typing import Protocol

from domains.eod_price.assets import record_price_dbt_materialization
from domains.exchange.assets import record_exchange_dbt_materialization
from domains.exchange_schedule.assets import record_exchange_schedule_dbt_materialization
from domains.fundamental.assets import record_fundamental_dbt_materialization
from domains.instrument.assets import record_instrument_dbt_materialization
from orchestration.domain_dbt import DOMAIN_DBT_SPECS

type DbtAssetGroup = str


class DbtAssetRecorder(Protocol):
    """Callable that records one domain's dbt materializations."""

    def __call__(self, **metadata: object) -> None:
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
    for group in selected_dbt_asset_groups(select):
        _DBT_ASSET_RECORDERS[group](**metadata)


def selected_dbt_asset_groups(select: Sequence[str]) -> list[DbtAssetGroup]:
    """Return dbt asset groups that match a dbt select expression."""
    if not select:
        return list(DBT_ASSET_GROUPS)

    joined = " ".join(select).lower()
    groups: list[DbtAssetGroup] = []
    for group, needles in _DBT_ASSET_SELECT_NEEDLES.items():
        if any(needle in joined for needle in needles):
            groups.append(group)
    return groups or list(DBT_ASSET_GROUPS)
