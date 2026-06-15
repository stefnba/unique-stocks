"""Tests for generic Prefect asset helpers."""

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
