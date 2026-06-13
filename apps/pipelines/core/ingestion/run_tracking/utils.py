"""Private normalization and runtime helpers for run tracking."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

from prefect.exceptions import Abort, CancelledRun, ExternalSignal, TerminationSignal

from core.ingestion.landing import LandingWrite
from core.ingestion.serialization import canonical_json, jsonable

_ERROR_LIMIT = 2000


def _landing_values(
    landing: LandingWrite | None,
    *,
    dataset: str | None,
    source_uri: str | None,
    partition: Mapping[str, object] | None,
    rows_raw: int | None,
    byte_count: int | None,
    content_hash: str | None,
) -> tuple[str, str, dict[str, object] | None, int | None, int | None, str | None]:
    if landing is not None:
        dataset = landing.dataset if dataset is None else dataset
        source_uri = landing.source_uri if source_uri is None else source_uri
        partition = landing.partition if partition is None and landing.partition is not None else partition
        rows_raw = landing.rows_raw if rows_raw is None else rows_raw
        byte_count = landing.byte_count if byte_count is None else byte_count
        content_hash = landing.content_hash if content_hash is None else content_hash
    if dataset is None or source_uri is None:
        raise ValueError("A landing object needs either LandingWrite metadata or dataset/source_uri arguments.")
    return dataset, source_uri, dict(partition) if partition is not None else None, rows_raw, byte_count, content_hash


def _current_prefect_flow_run_id() -> str | None:
    """Return the active Prefect flow run id when running inside Prefect."""
    try:
        from prefect.runtime import flow_run
    except ImportError:
        return None
    try:
        value = getattr(flow_run, "id", None)
    except Exception:
        return None
    return str(value) if value else None


def _code_version() -> str | None:
    """Return a configured source revision if the deployment exposes one."""
    for name in ("GIT_SHA", "SOURCE_COMMIT", "COMMIT_SHA", "IMAGE_TAG"):
        if value := os.getenv(name):
            return value
    return None


def _json_hash(value: dict[str, object]) -> str:
    return sha256(canonical_json(value).encode()).hexdigest()


def _json_dict(value: dict[str, object]) -> dict[str, object]:
    normalized = jsonable(value)
    if not isinstance(normalized, dict):
        raise TypeError("Expected JSON-normalized mapping")
    return normalized


def _raw_sample(value: object) -> dict[str, object] | None:
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        return {"items": value[:10]}
    if value is None:
        return None
    return {"value": value}


def _uuid_or_none(value: str | UUID | None) -> str | None:
    return str(value) if value is not None else None


def _is_cancelled_exception(exc: BaseException) -> bool:
    return isinstance(
        exc,
        (
            asyncio.CancelledError,
            Abort,
            CancelledRun,
            ExternalSignal,
            TerminationSignal,
        ),
    )


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)
