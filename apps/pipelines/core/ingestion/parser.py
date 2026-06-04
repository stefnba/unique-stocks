"""Parser helpers for turning provider payloads into Bronze parse results.

Use ``parse_strict_rows`` for small/reference payloads where every raw row is
expected to parse. Use ``parse_best_effort_rows`` for large or noisy provider
payloads where valid rows should continue even when some rows are rejected.
"""

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from core.clients.storage.s3.base import S3ObjectRef
from core.models import BronzeModel

type ParseFailureHandler[RawT] = Callable[[RawT, Exception], None]
type StrictRowBuilder[RawT, RowT: BronzeModel] = Callable[[RawT], RowT]
type BestEffortRowBuilder[RawT, RowT: BronzeModel] = Callable[[RawT], RowT | None]


@dataclass(frozen=True, slots=True)
class BronzeParseResult[RowT: BronzeModel]:
    """One parsed Bronze row plus the raw provider fragment that produced it.

    Attributes:
        row: Typed Bronze row produced by the parser.
        raw_fragment: Raw provider fragment used for lineage and rejection/debug samples.
        source_uri: Optional landing object URI that produced this result.
    """

    row: RowT
    raw_fragment: Any
    source_uri: str | None = None

    def with_source_uri(self, source_uri: str | S3ObjectRef) -> BronzeParseResult[RowT]:
        """Return this parse result with landing object lineage attached.

        Args:
            source_uri: Landing object URI or S3 reference.

        Returns:
            Copy of this result with ``source_uri`` populated.
        """
        return BronzeParseResult(row=self.row, raw_fragment=self.raw_fragment, source_uri=_source_uri(source_uri))


@dataclass(frozen=True, slots=True)
class BestEffortParseResult[RowT: BronzeModel, RawT]:
    """Rows accepted and rejected by a best-effort parser run.

    Attributes:
        valid: Parsed Bronze rows with their original raw fragments.
        rejected: Raw items that raised while parsing.
    """

    valid: list[BronzeParseResult[RowT]]
    rejected: list[RawT]


class BronzeParser[RawT, RowT: BronzeModel](Protocol):
    """Shape for parser objects that need shared context while building rows."""

    def parse(self, raw: RawT) -> RowT | None:
        """Parse one raw item, returning ``None`` when it should be skipped."""
        ...


def parse_result[RowT: BronzeModel](row: RowT, raw_fragment: Any) -> BronzeParseResult[RowT]:
    """Pair a parsed Bronze row with the raw provider fragment that produced it.

    Args:
        row: Typed Bronze row.
        raw_fragment: Raw provider fragment used to build ``row``.

    Returns:
        Parse result with no source URI attached yet.
    """
    return BronzeParseResult(row=row, raw_fragment=raw_fragment)


def parse_date(value: Any) -> date:
    """Parse a provider date scalar into a ``date``.

    Args:
        value: Date-like provider value, usually ISO ``YYYY-MM-DD``.

    Returns:
        Parsed date.

    Raises:
        ValueError: If the value cannot be converted to a date.
    """
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Cannot convert {value!r} to date") from exc


def parse_decimal(value: object) -> Decimal:
    """Parse a required provider numeric scalar into a ``Decimal``.

    Args:
        value: Required provider numeric value.

    Returns:
        Decimal representation of the value.

    Raises:
        ValueError: If the value is missing or not numeric.
    """
    if value is None:
        raise ValueError("Expected a numeric value, got None")
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Cannot convert {value!r} to Decimal") from exc


def parse_optional_decimal(value: object) -> Decimal | None:
    """Parse an optional provider numeric scalar into a ``Decimal`` when possible.

    Args:
        value: Optional provider numeric value.

    Returns:
        Decimal representation, or ``None`` for blanks/unparseable values.
    """
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def parse_strict_rows[RawT, RowT: BronzeModel](
    raws: Iterable[RawT],
    build_row: StrictRowBuilder[RawT, RowT],
) -> list[BronzeParseResult[RowT]]:
    """Parse raw rows with fail-fast semantics.

    Every raw row must build a Bronze row. Any exception raised by ``build_row``
    bubbles up and fails the whole parse call. Use this for small/reference
    payloads where a bad row usually means provider drift or a broken contract.

    Args:
        raws: Raw provider rows.
        build_row: Function that must return one Bronze row for each raw item.

    Returns:
        Parse results preserving every raw fragment.
    """
    return [parse_result(build_row(raw), raw) for raw in raws]


def parse_best_effort_rows[RawT, RowT: BronzeModel](
    raws: Iterable[RawT],
    build_row: BestEffortRowBuilder[RawT, RowT],
    *,
    on_rejected: ParseFailureHandler[RawT] | None = None,
) -> BestEffortParseResult[RowT, RawT]:
    """Parse raw rows with per-row isolation.

    ``build_row`` may return ``None`` to intentionally drop a raw item without
    counting it as rejected. Exceptions are caught per row, the original raw
    payload is added to ``rejected``, and parsing continues. Use this for large
    or noisy provider feeds where a few bad rows should not stop the batch.

    Args:
        raws: Raw provider rows.
        build_row: Function that returns a Bronze row, or ``None`` to drop an item.
        on_rejected: Optional callback invoked with the raw item and exception.

    Returns:
        Valid parse results and rejected raw items.
    """
    valid: list[BronzeParseResult[RowT]] = []
    rejected: list[RawT] = []
    for raw in raws:
        try:
            row = build_row(raw)
        except Exception as exc:
            rejected.append(raw)
            if on_rejected:
                on_rejected(raw, exc)
            continue

        if row is not None:
            valid.append(parse_result(row, raw))
    return BestEffortParseResult(valid=valid, rejected=rejected)


def attach_source_uri[RowT: BronzeModel](
    results: Sequence[BronzeParseResult[RowT]],
    source_uri: str | S3ObjectRef,
) -> list[BronzeParseResult[RowT]]:
    """Attach one landing object URI to parser-produced results.

    Args:
        results: Parse results to annotate.
        source_uri: Landing object URI or S3 reference.

    Returns:
        New parse results with ``source_uri`` attached.
    """
    return [result.with_source_uri(source_uri) for result in results]


def _source_uri(source: str | S3ObjectRef) -> str:
    if isinstance(source, S3ObjectRef):
        return source.uri
    return source


__all__ = [
    "BestEffortParseResult",
    "BronzeParser",
    "BronzeParseResult",
    "attach_source_uri",
    "parse_date",
    "parse_decimal",
    "parse_best_effort_rows",
    "parse_optional_decimal",
    "parse_result",
    "parse_strict_rows",
]
