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
