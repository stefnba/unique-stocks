"""Domain-owned Prefect asset materializations for dbt-built layers."""

from collections.abc import Sequence
from typing import Literal, Protocol

from domains.eod_price.assets import record_price_dbt_materialization
from domains.exchange.assets import record_exchange_dbt_materialization
from domains.exchange_schedule.assets import record_exchange_schedule_dbt_materialization
from domains.fundamental.assets import record_fundamental_dbt_materialization
from domains.instrument.assets import record_instrument_dbt_materialization

type DbtAssetGroup = Literal["exchange", "exchange_schedule", "instrument", "price", "fundamental"]


class DbtAssetRecorder(Protocol):
    """Callable that records one domain's dbt materializations."""

    def __call__(self, **metadata: object) -> None:
        """Record materialization metadata."""


DBT_ASSET_GROUPS: tuple[DbtAssetGroup, ...] = (
    "exchange",
    "exchange_schedule",
    "instrument",
    "price",
    "fundamental",
)

_DBT_ASSET_RECORDERS: dict[DbtAssetGroup, DbtAssetRecorder] = {
    "exchange": record_exchange_dbt_materialization,
    "exchange_schedule": record_exchange_schedule_dbt_materialization,
    "instrument": record_instrument_dbt_materialization,
    "price": record_price_dbt_materialization,
    "fundamental": record_fundamental_dbt_materialization,
}

_DBT_ASSET_SELECT_NEEDLES: dict[DbtAssetGroup, tuple[str, ...]] = {
    "exchange": ("exchange", "provider_namespace"),
    "exchange_schedule": ("exchange_schedule", "schedule", "holiday"),
    "instrument": ("instrument",),
    "price": ("price", "eod"),
    "fundamental": ("fundamental",),
}


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
