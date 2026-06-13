"""Tests for Prefect asset helpers."""

from core.prefect import assets as prefect_assets


def test_selected_dbt_asset_groups_defaults_to_all_for_full_build() -> None:
    """A full dbt build should record all configured Silver/Gold asset groups."""
    assert prefect_assets._selected_dbt_asset_groups([]) == [
        "exchange",
        "exchange_schedule",
        "instrument",
        "price",
        "fundamental",
    ]
