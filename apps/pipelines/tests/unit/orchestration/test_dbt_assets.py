"""Tests for app dbt Prefect asset materialization mapping."""

from orchestration.dbt_assets import selected_dbt_asset_groups


def test_selected_dbt_asset_groups_defaults_to_all_for_full_build() -> None:
    """A full dbt build should record all configured Silver/Gold asset groups."""
    assert selected_dbt_asset_groups([]) == [
        "exchange",
        "exchange_schedule",
        "instrument",
        "price",
        "fundamental",
    ]


def test_selected_dbt_asset_groups_matches_domain_selectors() -> None:
    """Dbt selector strings should map to the domain-owned asset groups they touch."""
    assert selected_dbt_asset_groups(["path:models/staging/price", "+path:models/marts/instrument"]) == [
        "instrument",
        "price",
    ]


def test_selected_dbt_asset_groups_ignores_unknown_specific_selectors() -> None:
    """Specific non-domain selectors should not record every Silver/Gold asset."""
    assert selected_dbt_asset_groups(["+tag:ingestion_control"]) == []
