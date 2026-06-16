"""Tests for generic dbt asset materialization helpers."""

from core.orchestration import dbt_assets
from core.orchestration.dbt_assets import DbtAssetMaterialization, DbtAssetSpec


def _specs(calls: list[dict[str, object]] | None = None) -> tuple[DbtAssetSpec, ...]:
    def record_price(*, layers: tuple[str, ...], **metadata: object) -> None:
        if calls is not None:
            calls.append({"group": "price", "layers": layers, "metadata": metadata})

    return (
        DbtAssetSpec(group="exchange", select_needles=("exchange",), recorder=lambda **_: None),
        DbtAssetSpec(group="instrument", select_needles=("instrument",), recorder=lambda **_: None),
        DbtAssetSpec(group="price", select_needles=("price", "eod"), recorder=record_price),
    )


def test_selected_dbt_asset_specs_defaults_to_all_for_full_build() -> None:
    """A full dbt build should record all configured Silver/Gold asset groups."""
    assert [spec.group for spec in dbt_assets.selected_dbt_asset_specs(select=[], specs=_specs())] == [
        "exchange",
        "instrument",
        "price",
    ]


def test_selected_dbt_asset_specs_matches_domain_selectors() -> None:
    """Dbt selector strings should map to the asset groups they touch."""
    assert [
        spec.group
        for spec in dbt_assets.selected_dbt_asset_specs(
            select=["path:models/staging/price", "+path:models/marts/instrument"],
            specs=_specs(),
        )
    ] == ["instrument", "price"]


def test_selected_dbt_asset_specs_ignores_unknown_specific_selectors() -> None:
    """Specific non-domain selectors should not record every Silver/Gold asset."""
    assert dbt_assets.selected_dbt_asset_specs(select=["+tag:ingestion_control"], specs=_specs()) == []


def test_selected_dbt_asset_specs_matches_whole_selector_tokens() -> None:
    """Selector fallback should not match shorter group names inside longer names."""
    specs = (
        DbtAssetSpec(group="exchange", select_needles=("exchange",), recorder=lambda **_: None),
        DbtAssetSpec(
            group="exchange_schedule",
            select_needles=("exchange_schedule", "schedule", "holiday"),
            recorder=lambda **_: None,
        ),
    )

    assert [
        spec.group
        for spec in dbt_assets.selected_dbt_asset_specs(
            select=["path:models/staging/exchange_schedule"],
            specs=specs,
        )
    ] == ["exchange_schedule"]


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

    assert dbt_assets.selected_dbt_asset_materializations(select=["tag:any"], metadata=metadata, specs=_specs()) == [
        DbtAssetMaterialization(
            group="instrument",
            layers=("gold",),
            models=(
                {
                    "unique_id": "model.unique_stocks.dim_instrument",
                    "original_file_path": "models/marts/instrument/dim_instrument.sql",
                },
            ),
        ),
        DbtAssetMaterialization(
            group="price",
            layers=("silver", "gold"),
            models=(
                {
                    "unique_id": "model.unique_stocks.stg_eod_price",
                    "original_file_path": "models/staging/price/stg_eod_price.sql",
                },
                {
                    "unique_id": "model.unique_stocks.fct_daily_price",
                    "original_file_path": "models/marts/price/fct_daily_price.sql",
                },
            ),
        ),
    ]


def test_record_dbt_asset_materializations_filters_metadata_by_asset() -> None:
    """Each recorder should receive only the models for its group and selected layers."""
    calls: list[dict[str, object]] = []

    dbt_assets.record_dbt_asset_materializations_for_specs(
        select=[],
        specs=_specs(calls),
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


def test_record_dbt_asset_materializations_preserves_run_count_when_model_paths_are_unavailable() -> None:
    """Selector fallback should not report zero asset models when manifest path metadata is missing."""
    calls: list[dict[str, object]] = []

    dbt_assets.record_dbt_asset_materializations_for_specs(
        select=["path:models/marts/price"],
        specs=_specs(calls),
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


def test_asset_metadata_for_materialization_ignores_models_without_unique_ids() -> None:
    """Generic metadata shaping should tolerate compact model payload variation."""
    payload = dbt_assets.asset_metadata_for_materialization(
        metadata={"dbt_run_id": "run-1", "dbt_materialized_model_count": 2},
        materialization=DbtAssetMaterialization(
            group="price",
            layers=("silver",),
            models=(
                {"original_file_path": "models/staging/price/stg_eod_price.sql"},
                {
                    "unique_id": "model.unique_stocks.stg_eod_price",
                    "original_file_path": "models/staging/price/stg_eod_price.sql",
                },
            ),
        ),
    )

    assert payload["dbt_asset_materialized_model_count"] == 2
    assert payload["dbt_materialized_model_unique_ids"] == ["model.unique_stocks.stg_eod_price"]
