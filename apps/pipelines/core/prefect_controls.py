from __future__ import annotations

import os
import sys
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from prefect.assets import Asset, AssetProperties, materialize
from prefect.concurrency.asyncio import rate_limit
from prefect.concurrency.sync import concurrency
from prefect.events import emit_event

logger = structlog.get_logger(__name__)

LAKE_WRITER_LIMIT = "unique-stocks.lake-writer"
PROVIDER_API_CREDIT_LIMIT = "unique-stocks.provider-api-credit"

DBT_FAILED_EVENT = "unique-stocks.dbt.failed"
COVERAGE_GATE_FAILED_EVENT = "unique-stocks.coverage-gate.failed"
PIPELINE_STALE_RUNNING_EVENT = "unique-stocks.pipeline.stale-running"
PIPELINE_CANCELLED_EVENT = "unique-stocks.pipeline.cancelled"


def _strict_limits() -> bool:
    return os.getenv("PREFECT_GLOBAL_LIMITS_STRICT", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


@contextmanager
def lake_writer_limit(operation: str | None = None) -> Generator[None]:
    """Serialize writes to the shared lake when Prefect limits are configured.

    The default is fail-open so local development and first-run bootstrap do not
    break if the Prefect server has not had limits created yet. Set
    PREFECT_GLOBAL_LIMITS_STRICT=true in production once limits are managed.
    """
    manager = concurrency(
        LAKE_WRITER_LIMIT,
        occupy=1,
        strict=_strict_limits(),
        raise_on_lease_renewal_failure=False,
    )
    try:
        manager.__enter__()
    except Exception as exc:
        if _strict_limits():
            raise
        logger.warning(
            "prefect_lake_writer_limit_unavailable",
            operation=operation,
            limit_name=LAKE_WRITER_LIMIT,
            error=str(exc),
        )
        yield
        return

    try:
        yield
    except BaseException:
        exc_type, exc, traceback = sys.exc_info()
        manager.__exit__(exc_type, exc, traceback)
        raise
    else:
        manager.__exit__(None, None, None)


async def wait_for_provider_api_credit(
    *,
    provider: str,
    operation: str | None = None,
) -> None:
    """Apply the shared provider/API-credit rate limit before outbound calls."""
    try:
        await rate_limit(
            PROVIDER_API_CREDIT_LIMIT,
            occupy=1,
            strict=_strict_limits(),
        )
    except Exception as exc:
        if _strict_limits():
            raise
        logger.warning(
            "prefect_provider_api_limit_unavailable",
            provider=provider,
            operation=operation,
            limit_name=PROVIDER_API_CREDIT_LIMIT,
            error=str(exc),
        )


def emit_pipeline_event(
    *,
    event: str,
    resource_id: str,
    resource_name: str,
    payload: dict[str, Any],
) -> None:
    """Emit one Prefect event without making the caller depend on the event API."""
    try:
        emit_event(
            event=event,
            resource={
                "prefect.resource.id": resource_id,
                "prefect.resource.name": resource_name,
            },
            payload=_jsonable(payload),
        )
    except Exception as exc:
        logger.warning(
            "prefect_event_emit_failed",
            event=event,
            resource_id=resource_id,
            error=str(exc),
        )


def emit_dbt_failed_event(
    *,
    dbt_run_id: str,
    app_run_id: str | None,
    command: str,
    target: str,
    return_code: int,
    failed_nodes: int,
    artifact_path: str | None,
) -> None:
    """Emit the canonical dbt failure event used by pipeline automations."""
    emit_pipeline_event(
        event=DBT_FAILED_EVENT,
        resource_id=f"unique-stocks.dbt-invocation.{dbt_run_id}",
        resource_name=f"dbt {command}",
        payload={
            "dbt_run_id": dbt_run_id,
            "app_run_id": app_run_id,
            "command": command,
            "target": target,
            "return_code": return_code,
            "failed_nodes": failed_nodes,
            "artifact_path": artifact_path,
        },
    )


def emit_coverage_gate_failed_event(
    *,
    app_run_id: str,
    gaps_count: int,
    provider_exchange_codes: Sequence[str],
    from_date: str,
    to_date: str,
) -> None:
    """Emit the EOD coverage-gate failure event used by pipeline automations."""
    emit_pipeline_event(
        event=COVERAGE_GATE_FAILED_EVENT,
        resource_id=f"unique-stocks.coverage-gate.{app_run_id}",
        resource_name="EOD coverage gate",
        payload={
            "app_run_id": app_run_id,
            "gaps_count": gaps_count,
            "provider_exchange_codes": list(provider_exchange_codes),
            "from_date": from_date,
            "to_date": to_date,
        },
    )


def emit_stale_runs_event(
    *,
    stale_runs: Sequence[dict[str, Any]],
    older_than_minutes: int,
) -> None:
    """Emit the stale-running audit event used by pipeline automations."""
    emit_pipeline_event(
        event=PIPELINE_STALE_RUNNING_EVENT,
        resource_id="unique-stocks.pipeline.runs",
        resource_name="Stale pipeline runs",
        payload={
            "older_than_minutes": older_than_minutes,
            "stale_run_count": len(stale_runs),
            "stale_runs": list(stale_runs),
        },
    )


def emit_pipeline_cancelled_event(
    *,
    app_run_id: str,
    flow_name: str,
    error_class: str,
    error_message: str,
) -> None:
    """Emit the pipeline cancellation event used by pipeline automations."""
    emit_pipeline_event(
        event=PIPELINE_CANCELLED_EVENT,
        resource_id=f"unique-stocks.pipeline-run.{app_run_id}",
        resource_name=flow_name,
        payload={
            "app_run_id": app_run_id,
            "flow_name": flow_name,
            "error_class": error_class,
            "error_message": error_message,
        },
    )


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/eod_price",
        properties=AssetProperties(
            name="Bronze EOD price",
            description="Provider-validated EOD price rows written by Python ingestion.",
        ),
    ),
    by="python",
    name="observe-bronze-eod-price-asset",
)
def _observe_bronze_eod_price_asset(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/exchange",
        properties=AssetProperties(name="Silver exchange"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/exchange",
        properties=AssetProperties(name="Gold exchange"),
    ),
    by="dbt",
    name="observe-dbt-exchange-assets",
)
def _observe_dbt_exchange_assets(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/instrument",
        properties=AssetProperties(name="Silver instrument"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/instrument",
        properties=AssetProperties(name="Gold instrument"),
    ),
    by="dbt",
    name="observe-dbt-instrument-assets",
)
def _observe_dbt_instrument_assets(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/price",
        properties=AssetProperties(name="Silver price"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/price",
        properties=AssetProperties(name="Gold price"),
    ),
    by="dbt",
    name="observe-dbt-price-assets",
)
def _observe_dbt_price_assets(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/silver/fundamental",
        properties=AssetProperties(name="Silver fundamental"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/fundamental",
        properties=AssetProperties(name="Gold fundamental"),
    ),
    by="dbt",
    name="observe-dbt-fundamental-assets",
)
def _observe_dbt_fundamental_assets(**metadata: Any) -> dict[str, Any]:
    return metadata


def observe_bronze_eod_price_asset(**metadata: Any) -> None:
    """Observe Bronze EOD price writes as a Prefect asset materialization."""
    try:
        _observe_bronze_eod_price_asset(**metadata)
    except Exception as exc:
        logger.warning(
            "prefect_bronze_asset_observation_failed",
            asset_key="duckdb://unique-stocks/bronze/eod_price",
            error=str(exc),
        )


def materialize_dbt_assets(
    *,
    select: Sequence[str],
    metadata: dict[str, Any],
) -> None:
    """Observe Silver/Gold dbt asset groups that were included in a successful build."""
    groups = _selected_dbt_asset_groups(select)
    observers = {
        "exchange": _observe_dbt_exchange_assets,
        "instrument": _observe_dbt_instrument_assets,
        "price": _observe_dbt_price_assets,
        "fundamental": _observe_dbt_fundamental_assets,
    }
    for group in groups:
        try:
            observers[group](**metadata)
        except Exception as exc:
            logger.warning(
                "prefect_dbt_asset_observation_failed",
                asset_group=group,
                error=str(exc),
            )


def _selected_dbt_asset_groups(select: Sequence[str]) -> list[str]:
    if not select:
        return ["exchange", "instrument", "price", "fundamental"]

    joined = " ".join(select).lower()
    groups: list[str] = []
    for group, needles in {
        "exchange": ("exchange", "provider_namespace"),
        "instrument": ("instrument",),
        "price": ("price", "eod"),
        "fundamental": ("fundamental",),
    }.items():
        if any(needle in joined for needle in needles):
            groups.append(group)
    return groups or ["exchange", "instrument", "price", "fundamental"]


def _jsonable(value: Any) -> Any:
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json")
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray | memoryview):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date | datetime | UUID):
        return str(value)
    return value
