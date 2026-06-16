"""Tests for app dbt Prefect asset materialization wiring."""

from orchestration import dbt_assets
from orchestration.dbt_assets import DbtAssetMaterialization, selected_dbt_asset_groups


def test_dbt_asset_specs_follow_domain_dbt_registry_order() -> None:
    """The app adapter should expose the configured domain asset groups."""
    assert [spec.group for spec in dbt_assets.DBT_ASSET_SPECS] == [
        "exchange",
        "exchange_schedule",
        "instrument",
        "price",
        "fundamental",
    ]


def test_selected_dbt_asset_groups_delegates_to_core_specs() -> None:
    """The app adapter should keep selector matching behavior at its public edge."""
    assert selected_dbt_asset_groups(["path:models/staging/price", "+path:models/marts/instrument"]) == [
        "instrument",
        "price",
    ]
    assert selected_dbt_asset_groups(["+tag:ingestion_control"]) == []


def test_selected_dbt_asset_materializations_uses_app_specs() -> None:
    """The app adapter should expose core materialization results with app specs."""
    metadata = {
        "dbt_materialized_models": [
            {
                "unique_id": "model.unique_stocks.stg_eod_price",
                "original_file_path": "models/staging/price/stg_eod_price.sql",
            }
        ]
    }

    assert dbt_assets.selected_dbt_asset_materializations(select=["tag:any"], metadata=metadata) == [
        DbtAssetMaterialization(
            group="price",
            layers=("silver",),
            models=(
                {
                    "unique_id": "model.unique_stocks.stg_eod_price",
                    "original_file_path": "models/staging/price/stg_eod_price.sql",
                },
            ),
        )
    ]


def test_record_dbt_asset_materializations_uses_app_recorders(monkeypatch) -> None:
    """The public hook should delegate through the app spec registry."""
    calls: list[dict[str, object]] = []

    def record_price(**metadata: object) -> None:
        calls.append(metadata)

    specs = tuple(
        spec if spec.group != "price" else spec.__class__(spec.group, spec.select_needles, record_price)
        for spec in dbt_assets.DBT_ASSET_SPECS
    )

    monkeypatch.setattr(dbt_assets, "DBT_ASSET_SPECS", specs)

    dbt_assets.record_dbt_asset_materializations(
        select=["path:models/staging/price"],
        metadata={"dbt_run_id": "run-1", "dbt_materialized_model_count": 1},
    )

    assert calls == [
        {
            "layers": ("silver",),
            "dbt_run_id": "run-1",
            "dbt_materialized_model_count": 1,
            "dbt_run_materialized_model_count": 1,
            "dbt_asset_group": "price",
            "dbt_asset_layers": ["silver"],
        }
    ]
