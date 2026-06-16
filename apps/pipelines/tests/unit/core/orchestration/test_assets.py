"""Tests for generic Prefect asset helpers."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from core.orchestration import assets


def test_record_prefect_materialization_calls_materializer(monkeypatch) -> None:
    """Generic wrapper should pass metadata through to the Prefect materializer."""
    calls: list[dict[str, object]] = []

    def materializer(**metadata: object) -> None:
        calls.append(metadata)

    monkeypatch.setattr(assets, "get_run_context", lambda: object())

    assets.record_prefect_materialization(
        materialization_name="bronze.demo",
        materializer=materializer,
        metadata={"rows_written": 3},
    )

    assert calls == [{"rows_written": 3}]


def test_record_prefect_materialization_skips_without_run_context() -> None:
    """Direct service tests should not emit Prefect artifacts without a run context."""
    calls: list[dict[str, object]] = []

    def materializer(**metadata: object) -> None:
        calls.append(metadata)

    assets.record_prefect_materialization(
        materialization_name="bronze.demo",
        materializer=materializer,
        metadata={"rows_written": 3},
    )

    assert calls == []


def test_infer_asset_change_kind_classifies_changed_unchanged_and_not_applicable() -> None:
    """Asset metadata should distinguish real writes from harmless no-op outcomes."""
    assert assets.infer_asset_change_kind(rows_written=3) == assets.AssetChangeKind.CHANGED
    assert assets.infer_asset_change_kind(rows_written=0, reason="already_ingested") == assets.AssetChangeKind.UNCHANGED
    assert (
        assets.infer_asset_change_kind(rows_written=0, reason="no_holiday", not_applicable_reasons=("no_holiday",))
        == assets.AssetChangeKind.NOT_APPLICABLE
    )
    assert (
        assets.infer_asset_change_kind(
            rows_written=0,
            reason="already_ingested,no_holiday",
            not_applicable_reasons=("no_holiday",),
        )
        == assets.AssetChangeKind.UNCHANGED
    )


def test_asset_materialization_metadata_standardizes_keys_and_change_kind() -> None:
    """Domain asset helpers should share one metadata contract."""
    payload = assets.asset_materialization_metadata(
        domain="price",
        layer="bronze",
        grain="table",
        rows_written=0,
        reason="no_bars",
        not_applicable_reasons=("no_bars",),
        snapshot_date=date(2026, 6, 15),
    )

    assert payload == {
        "asset_domain": "price",
        "asset_layer": "bronze",
        "asset_grain": "table",
        "rows_written": 0,
        "reason": "no_bars",
        "snapshot_date": "2026-06-15",
        "change_kind": "not_applicable",
    }


def test_asset_materialization_metadata_uses_dbt_model_count() -> None:
    """Dbt asset materializations should use model counts when row counts are absent."""
    payload = assets.asset_materialization_metadata(
        domain="price",
        layer="silver",
        grain="model_group",
        dbt_asset_materialized_model_count=0,
    )

    assert payload["change_kind"] == "unchanged"


def test_attach_asset_materialization_metadata_builds_and_attaches_standard_payload(monkeypatch) -> None:
    """Domain helpers should use the single-step attach helper for better DX."""
    calls: list[dict[str, object]] = []

    def add_metadata(asset: str, metadata: dict[str, object]) -> None:
        calls.append({"asset": asset, "metadata": metadata})

    monkeypatch.setattr(assets, "add_asset_metadata", add_metadata)

    payload = assets.attach_asset_materialization_metadata(
        "lakehouse://unique-stocks/bronze/demo",
        domain="demo",
        layer="bronze",
        grain="table",
        rows_written=1,
    )

    assert payload == {
        "asset_domain": "demo",
        "asset_layer": "bronze",
        "asset_grain": "table",
        "rows_written": 1,
        "change_kind": "changed",
    }
    assert calls == [{"asset": "lakehouse://unique-stocks/bronze/demo", "metadata": payload}]


def test_attach_materialization_metadata_jsonifies_common_values(monkeypatch) -> None:
    """Asset metadata should be safe for Prefect event payloads."""
    calls: list[dict[str, object]] = []

    def add_metadata(asset: str, metadata: dict[str, object]) -> None:
        calls.append({"asset": asset, "metadata": metadata})

    monkeypatch.setattr(assets, "add_asset_metadata", add_metadata)

    payload = assets.attach_materialization_metadata(
        "lakehouse://unique-stocks/bronze/demo",
        {
            "trade_date": date(2026, 6, 15),
            "completed_at": datetime(2026, 6, 15, 12, 30),
            "run_id": UUID("00000000-0000-0000-0000-000000000123"),
            "ratio": Decimal("1.25"),
            "nested": {"values": [Decimal("2.5")]},
        },
    )

    assert payload == {
        "trade_date": "2026-06-15",
        "completed_at": "2026-06-15 12:30:00",
        "run_id": "00000000-0000-0000-0000-000000000123",
        "ratio": "1.25",
        "nested": {"values": ["2.5"]},
    }
    assert calls == [{"asset": "lakehouse://unique-stocks/bronze/demo", "metadata": payload}]


def test_attach_materialization_metadata_skips_without_asset_context() -> None:
    """Direct task-body tests should keep working outside Prefect's asset context."""
    payload = assets.attach_materialization_metadata(
        "lakehouse://unique-stocks/bronze/demo",
        {"rows_written": 3},
    )

    assert payload == {"rows_written": 3}
