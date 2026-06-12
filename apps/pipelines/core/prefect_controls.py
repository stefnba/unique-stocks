from __future__ import annotations

import inspect
import json
import os
import re
import sys
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import structlog
from prefect.artifacts import create_markdown_artifact
from prefect.assets import Asset, AssetProperties, materialize
from prefect.concurrency.asyncio import rate_limit
from prefect.concurrency.sync import concurrency
from prefect.events import emit_event

logger = structlog.get_logger(__name__)

LAKE_WRITER_LIMIT = "unique-stocks.lake-writer"
PROVIDER_API_CREDIT_LIMIT = "unique-stocks.provider-api-credit"

DBT_FAILED_EVENT = "unique-stocks.dbt.failed"
COVERAGE_GATE_FAILED_EVENT = "unique-stocks.coverage-gate.failed"
INGESTION_PARTIAL_EVENT = "unique-stocks.ingestion.partial"
INGESTION_FAILED_EVENT = "unique-stocks.ingestion.failed"
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


def emit_ingestion_status_event(
    *,
    flow_name: str,
    domain: str,
    app_run_id: str,
    status: str,
    summary: dict[str, Any],
) -> None:
    """Emit a generic ingestion partial/failed event for operator automation."""
    if status not in {"partial", "failed"}:
        return
    emit_pipeline_event(
        event=INGESTION_PARTIAL_EVENT if status == "partial" else INGESTION_FAILED_EVENT,
        resource_id=f"unique-stocks.ingestion-run.{app_run_id}",
        resource_name=flow_name,
        payload={
            "app_run_id": app_run_id,
            "flow_name": flow_name,
            "domain": domain,
            "status": status,
            "summary": summary,
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


async def publish_ingestion_observability(
    *,
    flow_name: str,
    domain: str,
    app_run_id: str,
    status: str,
    summary: dict[str, Any],
) -> None:
    """Publish a compact ingestion summary artifact and any terminal alert event."""
    await _create_ingestion_summary_artifact(
        flow_name=flow_name,
        domain=domain,
        app_run_id=app_run_id,
        status=status,
        summary=summary,
    )
    emit_ingestion_status_event(
        flow_name=flow_name,
        domain=domain,
        app_run_id=app_run_id,
        status=status,
        summary=summary,
    )


async def _create_ingestion_summary_artifact(
    *,
    flow_name: str,
    domain: str,
    app_run_id: str,
    status: str,
    summary: dict[str, Any],
) -> None:
    try:
        summary_json = json.dumps(_jsonable(summary), default=str, indent=2, sort_keys=True)
        if len(summary_json) > 12_000:
            summary_json = f"{summary_json[:12_000]}\n... truncated ..."
        body = "\n".join(
            [
                f"# {flow_name} {status}",
                "",
                f"- Domain: `{domain}`",
                f"- App run: `{app_run_id}`",
                f"- Status: `{status}`",
                "",
                "```json",
                summary_json,
                "```",
            ]
        )
        artifact_id = create_markdown_artifact(
            key=f"ingestion-{_slug(flow_name)}-{_slug(app_run_id)[:16]}",
            markdown=body,
            description=f"{flow_name} ingestion summary ({status}).",
        )
        if inspect.isawaitable(artifact_id):
            await artifact_id
    except Exception as exc:
        logger.warning(
            "prefect_ingestion_summary_artifact_failed",
            flow_name=flow_name,
            domain=domain,
            run_id=app_run_id,
            error=str(exc),
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
        key="duckdb://unique-stocks/bronze/exchange_catalog",
        properties=AssetProperties(name="Bronze exchange catalog"),
    ),
    by="python",
    name="observe-bronze-exchange-catalog-asset",
)
def _observe_bronze_exchange_catalog_asset(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/exchange_mic_registry",
        properties=AssetProperties(name="Bronze exchange MIC registry"),
    ),
    by="python",
    name="observe-bronze-exchange-mic-registry-asset",
)
def _observe_bronze_exchange_mic_registry_asset(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/exchange_schedule",
        properties=AssetProperties(name="Bronze exchange schedule"),
    ),
    by="python",
    name="observe-bronze-exchange-schedule-asset",
)
def _observe_bronze_exchange_schedule_asset(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/exchange_holiday",
        properties=AssetProperties(name="Bronze exchange holiday"),
    ),
    by="python",
    name="observe-bronze-exchange-holiday-asset",
)
def _observe_bronze_exchange_holiday_asset(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/instrument",
        properties=AssetProperties(name="Bronze instrument"),
    ),
    by="python",
    name="observe-bronze-instrument-asset",
)
def _observe_bronze_instrument_asset(**metadata: Any) -> dict[str, Any]:
    return metadata


@materialize(
    Asset(
        key="duckdb://unique-stocks/bronze/fundamental",
        properties=AssetProperties(
            name="Bronze fundamental",
            description="Aggregate observation for bronze.fundamental_* tables.",
        ),
    ),
    by="python",
    name="observe-bronze-fundamental-asset",
)
def _observe_bronze_fundamental_asset(**metadata: Any) -> dict[str, Any]:
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
        key="duckdb://unique-stocks/silver/exchange_schedule",
        properties=AssetProperties(name="Silver exchange schedule"),
    ),
    Asset(
        key="duckdb://unique-stocks/gold/exchange_schedule",
        properties=AssetProperties(name="Gold exchange schedule"),
    ),
    by="dbt",
    name="observe-dbt-exchange-schedule-assets",
)
def _observe_dbt_exchange_schedule_assets(**metadata: Any) -> dict[str, Any]:
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
    observe_bronze_assets(["eod_price"], metadata=metadata)


def observe_bronze_assets(
    asset_names: Sequence[str],
    *,
    metadata: dict[str, Any],
) -> None:
    """Observe one or more Bronze asset materializations by logical asset name."""
    observers: dict[str, Callable[..., Any]] = {
        "eod_price": _observe_bronze_eod_price_asset,
        "exchange_catalog": _observe_bronze_exchange_catalog_asset,
        "exchange_mic_registry": _observe_bronze_exchange_mic_registry_asset,
        "exchange_schedule": _observe_bronze_exchange_schedule_asset,
        "exchange_holiday": _observe_bronze_exchange_holiday_asset,
        "instrument": _observe_bronze_instrument_asset,
        "fundamental": _observe_bronze_fundamental_asset,
    }
    for asset_name in asset_names:
        observer = observers.get(asset_name)
        if observer is None:
            logger.warning("prefect_unknown_bronze_asset", asset_name=asset_name)
            continue
        _observe_bronze_asset(asset_name=asset_name, observer=observer, metadata=metadata)


def _observe_bronze_asset(
    *,
    asset_name: str,
    observer: Callable[..., Any],
    metadata: dict[str, Any],
) -> None:
    try:
        observer(**metadata)
    except Exception as exc:
        logger.warning(
            "prefect_bronze_asset_observation_failed",
            asset_name=asset_name,
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
        "exchange_schedule": _observe_dbt_exchange_schedule_assets,
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
        return ["exchange", "exchange_schedule", "instrument", "price", "fundamental"]

    joined = " ".join(select).lower()
    groups: list[str] = []
    for group, needles in {
        "exchange": ("exchange", "provider_namespace"),
        "exchange_schedule": ("exchange_schedule", "schedule", "holiday"),
        "instrument": ("instrument",),
        "price": ("price", "eod"),
        "fundamental": ("fundamental",),
    }.items():
        if any(needle in joined for needle in needles):
            groups.append(group)
    return groups or ["exchange", "exchange_schedule", "instrument", "price", "fundamental"]


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9-]+", "-", value).strip("-").lower()
    return slug or "run"


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
