"""Display formatting helpers for the pipeline audit dashboard."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, cast

from dashboard.constants import ATTENTION_STATUSES, HEALTHY_RUN_STATUSES


def format_window(hours: int) -> str:
    """Format a dashboard time window length for sidebar display.

    Args:
        hours: Window length in hours.

    Returns:
        Human-readable label such as ``7 days`` or ``24 hours``.
    """
    if hours < 24:
        return f"{hours} hours"
    days = hours // 24
    return f"{days} day{'s' if days != 1 else ''}"


def format_int(value: object) -> str:
    """Format an integer-like value with thousands separators.

    Args:
        value: Numeric value to format. ``None`` is treated as zero.

    Returns:
        Comma-separated integer string.
    """
    if value is None:
        return "0"
    return f"{int(cast(Any, value)):,}"


def format_status(value: object) -> str:
    """Format a run or unit status for operator-facing labels.

    Args:
        value: Raw status string from the audit tables.

    Returns:
        Status text, optionally suffixed with health guidance.
    """
    status = str(value or "-")
    if status in ATTENTION_STATUSES:
        return f"{status} needs attention"
    if status in HEALTHY_RUN_STATUSES:
        return f"{status} healthy"
    return status


def format_unit_count(run: dict[str, Any]) -> str:
    """Format succeeded-over-total unit counts for a run summary.

    Args:
        run: Run row containing ``units_succeeded`` and ``units_total``.

    Returns:
        Count string such as ``50/73`` or a single total when succeeded is absent.
    """
    succeeded = run.get("units_succeeded")
    total = run.get("units_total")
    if total is None:
        return "0"
    if succeeded is None:
        return format_int(total)
    return f"{format_int(succeeded)}/{format_int(total)}"


def format_target_window(run: dict[str, Any]) -> str:
    """Format the logical target window recorded on a pipeline run.

    Args:
        run: Run row containing ``target_window_start`` and ``target_window_end``.

    Returns:
        Compact target-window label, or an empty string when both bounds are missing.
    """
    start = run.get("target_window_start")
    end = run.get("target_window_end")
    if not start and not end:
        return ""
    if start and end:
        return f"target {start} to {end}"
    if start:
        return f"target from {start}"
    return f"target to {end}"


def format_duration(value: object) -> str:
    """Format a duration in seconds for dashboard tables and metrics.

    Args:
        value: Duration in seconds. ``None`` renders as ``-``.

    Returns:
        Compact duration string such as ``2m 47s`` or ``1h 5m``.
    """
    if value is None:
        return "-"
    seconds = max(0, int(cast(Any, value)))
    minutes, remaining_seconds = divmod(seconds, 60)
    hours, remaining_minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {remaining_minutes}m"
    if minutes:
        return f"{minutes}m {remaining_seconds}s"
    return f"{remaining_seconds}s"


def format_datetime(value: object) -> str:
    """Format a timestamp for UTC display in dashboard tables.

    Args:
        value: ``datetime`` or string-like timestamp. ``None`` and NaT render as ``-``.

    Returns:
        Timestamp formatted as ``YYYY-MM-DD HH:MM UTC``.
    """
    if value is None:
        return "-"
    if type(value).__name__ == "NaTType":
        return "-"
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")
    return str(value)


def format_age_since(value: object) -> str | None:
    """Format elapsed time since a timestamp.

    Args:
        value: Past ``datetime`` value.

    Returns:
        Relative age string such as ``1h 25m ago``, or ``None`` when unavailable.
    """
    if value is None or type(value).__name__ == "NaTType":
        return None
    if isinstance(value, datetime):
        delta_seconds = max(0, int((datetime.now(UTC) - value.astimezone(UTC)).total_seconds()))
        return f"{format_duration(delta_seconds)} ago"
    return None


def format_json_compact(value: object) -> str:
    """Serialize JSON-like values into a compact single-line string.

    Args:
        value: JSON object, JSON string, or scalar value.

    Returns:
        Compact JSON/text representation suitable for dataframe cells.
    """
    if value is None:
        return "-"
    json_value = json_value_parsed(value)
    if isinstance(json_value, dict | list):
        return json.dumps(json_value, default=str, separators=(",", ":"))
    return str(json_value)


def format_key_value(value: object) -> str:
    """Format one unit-key field for cards and parsed key columns.

    Args:
        value: Scalar, datetime, or nested JSON fragment from a unit key.

    Returns:
        Display string for the field value.
    """
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return format_datetime(value)
    if isinstance(value, dict | list):
        return format_json_compact(value)
    return str(value)


def humanize_column(value: object) -> str:
    """Convert a snake_case column name into title case.

    Args:
        value: Raw column identifier.

    Returns:
        Human-readable column label.
    """
    return str(value).replace("_", " ").strip().title()


def truncate_text(value: object, *, limit: int = 96) -> str:
    """Truncate long text for compact table cells.

    Args:
        value: Text or message value to display.
        limit: Maximum number of characters before truncation.

    Returns:
        Original text or a truncated variant ending with ``...``.
    """
    if value is None or type(value).__name__ == "NaTType":
        return "-"
    text = str(value)
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1]}..."


def safe_key(value: object) -> str:
    """Sanitize an arbitrary value for use in Streamlit widget keys.

    Args:
        value: Raw identifier such as a run or unit UUID.

    Returns:
        Alphanumeric/underscore string safe for Streamlit keys.
    """
    return "".join(character if character.isalnum() else "_" for character in str(value))


def short_id(value: object) -> str:
    """Format a durable identifier as a short dashboard label.

    Args:
        value: Full UUID or identifier string.

    Returns:
        Short identifier such as ``81198e62...a49b``, or ``-`` when missing.
    """
    if value is None:
        return "-"
    text = str(value)
    if len(text) <= 12:
        return text
    return f"{text[:8]}...{text[-4:]}"


def json_value_parsed(value: object) -> object:
    """Parse JSON payloads stored as strings in audit tables.

    Args:
        value: JSON string, mapping, or ``None``.

    Returns:
        Parsed JSON value. Invalid JSON strings are returned unchanged.
    """
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return jsonable(value)


def jsonable(value: object) -> object:
    """Recursively convert values into JSON-serializable structures.

    Args:
        value: Arbitrary Python value from lake rows or Streamlit payloads.

    Returns:
        JSON-safe structure with string keys and ISO-formatted datetimes.
    """
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def json_dict(value: object) -> dict[str, Any]:
    """Parse a value into a string-keyed dictionary when possible.

    Args:
        value: JSON payload from ``unit_key_json`` or similar audit columns.

    Returns:
        Parsed dictionary, or an empty dict when parsing fails.
    """
    json_value = json_value_parsed(value)
    if isinstance(json_value, dict):
        return {str(key): item for key, item in json_value.items()}
    return {}


def format_unit_title(unit: dict[str, Any]) -> str:
    """Build a human-readable unit title from parsed key fields.

    Args:
        unit: Work-unit row containing ``unit_key_json`` and ``unit_type``.

    Returns:
        Title derived from unit-key values, falling back to the unit type.
    """
    unit_key = json_dict(unit.get("unit_key_json"))
    if unit_key:
        parts = [format_key_value(item) for item in unit_key.values() if item is not None]
        if parts:
            return " · ".join(parts)
    unit_type = unit.get("unit_type")
    if unit_type:
        return f"{unit_type} unit"
    return "Work unit"


def error_text(label: str, *, error_class: object, error_message: object) -> str:
    """Build a concise operator-facing error sentence.

    Args:
        label: Leading context such as ``Run failed`` or ``Unit failed``.
        error_class: Optional exception or error class name.
        error_message: Optional error message text.

    Returns:
        Combined error string with whichever fields are present.
    """
    if error_class and error_message:
        return f"{label}: {error_class}: {error_message}"
    if error_class:
        return f"{label}: {error_class}"
    if error_message:
        return f"{label}: {error_message}"
    return label
