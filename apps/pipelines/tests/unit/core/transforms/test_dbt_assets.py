"""Tests for dbt Prefect asset materialization mapping."""

from core.transforms.dbt_assets import selected_dbt_asset_groups


def test_selected_dbt_asset_groups_defaults_to_all_for_full_build() -> None:
    """A full dbt build should record all configured Silver/Gold asset groups."""
    assert selected_dbt_asset_groups([]) == [
        "exchange",
        "exchange_schedule",
        "instrument",
        "price",
        "fundamental",
    ]
