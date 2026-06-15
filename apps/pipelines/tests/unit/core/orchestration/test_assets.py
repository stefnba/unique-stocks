"""Tests for generic Prefect asset helpers."""

from core.orchestration.assets import record_prefect_materialization


def test_record_prefect_materialization_calls_materializer() -> None:
    """Generic wrapper should pass metadata through to the Prefect materializer."""
    calls: list[dict[str, object]] = []

    def materializer(**metadata: object) -> None:
        calls.append(metadata)

    record_prefect_materialization(
        materialization_name="bronze.demo",
        materializer=materializer,
        metadata={"rows_written": 3},
    )

    assert calls == [{"rows_written": 3}]
