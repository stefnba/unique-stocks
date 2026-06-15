"""Tests for app dbt Prefect asset materialization mapping."""

from orchestration import dbt_assets
from orchestration.dbt_assets import DbtAssetMaterialization, selected_dbt_asset_groups


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


def test_selected_dbt_asset_materializations_uses_model_paths() -> None:
    """Dbt model metadata should drive precise domain and layer materializations."""
    metadata = {
        "dbt_materialized_models": [
            {
                "unique_id": "model.unique_stocks.stg_eod_price",
                "original_file_path": "models/staging/price/stg_eod_price.sql",
            },
            {
                "unique_id": "model.unique_stocks.fct_daily_price",
                "original_file_path": "models/marts/price/fct_daily_price.sql",
            },
            {
                "unique_id": "model.unique_stocks.dim_instrument",
                "original_file_path": "models/marts/instrument/dim_instrument.sql",
            },
        ]
    }

    assert dbt_assets.selected_dbt_asset_materializations(select=["tag:any"], metadata=metadata) == [
        DbtAssetMaterialization(group="instrument", layers=("gold",)),
        DbtAssetMaterialization(group="price", layers=("silver", "gold")),
    ]


def test_record_dbt_asset_materializations_filters_metadata_by_asset(monkeypatch) -> None:
    """Each domain recorder should receive only the models for its group and selected layers."""
    calls: list[dict[str, object]] = []

    def record_price(*, layers: tuple[str, ...], **metadata: object) -> None:
        calls.append({"group": "price", "layers": layers, "metadata": metadata})

    monkeypatch.setitem(dbt_assets._DBT_ASSET_RECORDERS, "price", record_price)

    dbt_assets.record_dbt_asset_materializations(
        select=[],
        metadata={
            "dbt_run_id": "run-1",
            "dbt_materialized_model_count": 2,
            "dbt_materialized_models": [
                {
                    "unique_id": "model.unique_stocks.stg_eod_price",
                    "original_file_path": "models/staging/price/stg_eod_price.sql",
                },
                {
                    "unique_id": "model.unique_stocks.dim_exchange",
                    "original_file_path": "models/marts/exchange/dim_exchange.sql",
                },
            ],
        },
    )

    assert calls == [
        {
            "group": "price",
            "layers": ("silver",),
            "metadata": {
                "dbt_run_id": "run-1",
                "dbt_materialized_model_count": 1,
                "dbt_run_materialized_model_count": 2,
                "dbt_asset_group": "price",
                "dbt_asset_layers": ["silver"],
                "dbt_asset_materialized_model_count": 1,
                "dbt_materialized_models": [
                    {
                        "unique_id": "model.unique_stocks.stg_eod_price",
                        "original_file_path": "models/staging/price/stg_eod_price.sql",
                    }
                ],
                "dbt_materialized_model_unique_ids": ["model.unique_stocks.stg_eod_price"],
            },
        }
    ]


def test_record_dbt_asset_materializations_preserves_run_count_when_model_paths_are_unavailable(
    monkeypatch,
) -> None:
    """Selector fallback should not report zero asset models when manifest path metadata is missing."""
    calls: list[dict[str, object]] = []

    def record_price(*, layers: tuple[str, ...], **metadata: object) -> None:
        calls.append({"group": "price", "layers": layers, "metadata": metadata})

    monkeypatch.setitem(dbt_assets._DBT_ASSET_RECORDERS, "price", record_price)

    dbt_assets.record_dbt_asset_materializations(
        select=["path:models/marts/price"],
        metadata={
            "dbt_run_id": "run-1",
            "dbt_materialized_model_count": 2,
            "dbt_materialized_models": [
                {"unique_id": "model.unique_stocks.fct_daily_price"},
                {"unique_id": "model.unique_stocks.dim_instrument"},
            ],
        },
    )

    assert calls == [
        {
            "group": "price",
            "layers": ("gold",),
            "metadata": {
                "dbt_run_id": "run-1",
                "dbt_materialized_model_count": 2,
                "dbt_run_materialized_model_count": 2,
                "dbt_asset_group": "price",
                "dbt_asset_layers": ["gold"],
            },
        }
    ]
