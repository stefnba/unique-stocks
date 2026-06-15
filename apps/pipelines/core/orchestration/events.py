from enum import StrEnum


class PrefectEvent(StrEnum):
    """Canonical Prefect event names emitted and automated by this app."""

    # dbt
    DBT_FAILED = "unique-stocks.dbt.failed"
    # coverage gate
    COVERAGE_GATE_FAILED = "unique-stocks.coverage-gate.failed"
    # ingestion
    INGESTION_PARTIAL = "unique-stocks.ingestion.partial"
    INGESTION_FAILED = "unique-stocks.ingestion.failed"
    # pipeline
    PIPELINE_STALE_RUNNING = "unique-stocks.pipeline.stale-running"
    PIPELINE_CANCELLED = "unique-stocks.pipeline.cancelled"
