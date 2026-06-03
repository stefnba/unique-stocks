"""Cross-domain pipeline coverage for resumable ingestion partitions.

``pipeline.ingestion_coverage`` stores durable terminal outcomes per work unit
(aligned with ``pipeline.run_units`` grain) so flows can skip partitions on
re-run - for example after HTTP 429 quota exhaustion or an empty provider result.

Coverage rows are pipeline metadata, not bronze market data. Domains choose
``unit_type`` and ``unit_key`` conventions; see domain READMEs for examples.
When callers pass ``unit_key_matches``, helpers push JSON-field predicates into
the lake query before applying a Python fallback filter.
"""

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from hashlib import sha256
from typing import Any, Literal, Protocol
from uuid import UUID

from core.ingestion.serialization import canonical_json, jsonable
from lake.schema import PIPELINE_INGESTION_COVERAGE_TABLE, PipelineIngestionCoverageRow

INGESTION_COVERAGE_TABLE_NAME = PIPELINE_INGESTION_COVERAGE_TABLE.table_name

CoverageStatus = Literal["no_data", "provider_quota_deferred"]
COVERAGE_STATUS_NO_DATA: CoverageStatus = "no_data"
COVERAGE_STATUS_PROVIDER_QUOTA_DEFERRED: CoverageStatus = "provider_quota_deferred"


class IngestionCoverageLake(Protocol):
    """Minimal lake interface needed by coverage helpers."""

    def qualified_name(self, schema: str, table: str) -> str:
        """Return a fully qualified table name."""
        ...

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Run a SELECT and return one row."""
        ...

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Run a SELECT and return rows."""
        ...

    def insert_rows(self, schema: str, table: str, rows: list[dict[str, Any]]) -> int:
        """Insert rows into a table."""
        ...


def unit_key_hash(unit_key: Mapping[str, object]) -> str:
    """Return a stable hash for a JSON unit key (same algorithm as run units)."""
    normalized = jsonable(unit_key)
    if not isinstance(normalized, dict):
        raise TypeError("unit_key must serialize to a JSON object")
    return sha256(canonical_json(normalized).encode()).hexdigest()


def normalized_unit_key(unit_key: Mapping[str, object]) -> dict[str, object]:
    """Normalize a unit key for storage and hashing."""
    normalized = jsonable(unit_key)
    if not isinstance(normalized, dict):
        raise TypeError("unit_key must serialize to a JSON object")
    return normalized


def ingestion_coverage_recorded(
    lake: IngestionCoverageLake,
    *,
    domain: str,
    provider: str,
    unit_type: str,
    unit_key: Mapping[str, object],
    status: CoverageStatus,
) -> bool:
    """Return True when a coverage row already exists for this partition and status."""
    qualified = lake.qualified_name("pipeline", INGESTION_COVERAGE_TABLE_NAME)
    row = lake.query_one(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {qualified}
        WHERE domain = ?
          AND provider = ?
          AND unit_type = ?
          AND unit_key_hash = ?
          AND status = ?
        """,
        [domain, provider, unit_type, unit_key_hash(unit_key), status],
    )
    return bool(row and row["cnt"] > 0)


def record_ingestion_coverage(
    lake: IngestionCoverageLake,
    *,
    run_id: str | UUID,
    domain: str,
    provider: str,
    unit_type: str,
    unit_key: Mapping[str, object],
    status: CoverageStatus,
    reason: str | None = None,
    rows_raw: int | None = None,
    rows_valid: int | None = None,
    rows_rejected: int | None = None,
    source_uri: str | None = None,
    recorded_at: datetime,
) -> int:
    """Insert one coverage row when the partition is not already recorded for ``status``.

    Returns:
        ``1`` when a row was inserted, ``0`` when the partition was already recorded.
    """
    if ingestion_coverage_recorded(
        lake,
        domain=domain,
        provider=provider,
        unit_type=unit_type,
        unit_key=unit_key,
        status=status,
    ):
        return 0

    key = normalized_unit_key(unit_key)
    payload = PipelineIngestionCoverageRow(
        run_id=UUID(str(run_id)),
        domain=domain,
        provider=provider,
        unit_type=unit_type,
        unit_key_hash=unit_key_hash(key),
        unit_key_json=key,
        status=status,
        reason=reason,
        rows_raw=rows_raw,
        rows_valid=rows_valid,
        rows_rejected=rows_rejected,
        source_uri=source_uri,
        recorded_at=recorded_at,
    ).model_dump(mode="json")
    return lake.insert_rows("pipeline", INGESTION_COVERAGE_TABLE_NAME, [payload])


def list_ingestion_coverage_unit_keys(
    lake: IngestionCoverageLake,
    *,
    domain: str,
    provider: str,
    unit_type: str,
    status: CoverageStatus,
    unit_key_matches: Mapping[str, object] | None = None,
) -> list[dict[str, object]]:
    """Return unit keys for coverage rows, optionally filtered by JSON field equality.

    ``unit_key_matches`` compares stringified values to keys on ``unit_key_json``
    (for example ``{"from_date": "2026-05-01", "provider_exchange_code": "US"}``).
    DuckDB/MotherDuck can apply those matches through ``json_extract_string`` so
    large coverage tables are filtered before rows are returned to Python. The
    in-process filter remains as a defensive fallback and for fake lake tests.
    """
    qualified = lake.qualified_name("pipeline", INGESTION_COVERAGE_TABLE_NAME)
    params: list[object] = [domain, provider, unit_type, status]
    matches = _normalized_matches(unit_key_matches)
    match_clauses: list[str] = []
    for key, expected in matches.items():
        match_clauses.append("          AND json_extract_string(unit_key_json, ?) = ?")
        params.extend([_json_path_for_key(key), expected])
    match_sql = "\n".join(match_clauses)
    rows = lake.query(
        f"""
        SELECT unit_key_json
        FROM {qualified}
        WHERE domain = ?
          AND provider = ?
          AND unit_type = ?
          AND status = ?
{match_sql}
        """,
        params,
    )
    if not unit_key_matches:
        return [unit_key for row in rows if (unit_key := _coerce_unit_key(row.get("unit_key_json"))) is not None]

    filtered: list[dict[str, object]] = []
    for row in rows:
        unit_key = _coerce_unit_key(row.get("unit_key_json"))
        if unit_key is None:
            continue
        if all(str(jsonable(unit_key.get(key))) == expected for key, expected in matches.items()):
            filtered.append(unit_key)
    return filtered


def _normalized_matches(unit_key_matches: Mapping[str, object] | None) -> dict[str, str]:
    """Normalize unit-key match values to provider-independent strings."""
    if not unit_key_matches:
        return {}
    return {key: str(jsonable(value)) for key, value in unit_key_matches.items()}


def _json_path_for_key(key: str) -> str:
    """Return a DuckDB JSON path for a top-level object key."""
    return f"$.{json.dumps(key)}"


def _coerce_unit_key(value: object) -> dict[str, object] | None:
    """Convert a stored JSON unit key into a dictionary."""
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(decoded, dict):
            return decoded
    return None
