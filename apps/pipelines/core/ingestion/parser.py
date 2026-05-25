"""Parser-facing ingestion types."""

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from core.clients.storage.s3.base import S3ObjectRef
from core.models import BronzeModel

type ParseFailureHandler[RawT] = Callable[[RawT, Exception], None]
type ParseOne[RawT, RowT: BronzeModel] = Callable[[RawT], BronzeParseResult[RowT] | None]
type RowBuilder[RawT, RowT: BronzeModel] = Callable[[RawT], RowT]


@dataclass(frozen=True, slots=True)
class BronzeParseResult[RowT: BronzeModel]:
    """One parsed Bronze row plus the raw provider fragment that produced it."""

    row: RowT
    raw_fragment: Any
    source_uri: str | None = None

    def with_source_uri(self, source_uri: str | S3ObjectRef) -> BronzeParseResult[RowT]:
        """Return this parse result with landing object lineage attached."""
        return BronzeParseResult(row=self.row, raw_fragment=self.raw_fragment, source_uri=_source_uri(source_uri))


class BronzeParser[RawT, RowT: BronzeModel](Protocol):
    """Shape for parser objects that parse one raw item into one Bronze row."""

    def parse(self, raw: RawT) -> BronzeParseResult[RowT] | None:
        """Parse one raw item, returning ``None`` when it should be skipped."""
        ...


def parse_result[RowT: BronzeModel](row: RowT, raw_fragment: Any) -> BronzeParseResult[RowT]:
    """Pair a parsed Bronze row with the raw provider fragment that produced it."""
    return BronzeParseResult(row=row, raw_fragment=raw_fragment)


def parse_rows[RawT, RowT: BronzeModel](
    raws: Iterable[RawT],
    build_row: RowBuilder[RawT, RowT],
) -> list[BronzeParseResult[RowT]]:
    """Parse one-to-one raw rows while keeping each raw fragment for lineage."""
    return [parse_result(build_row(raw), raw) for raw in raws]


def parse_many[RawT, RowT: BronzeModel](
    raws: Iterable[RawT],
    parse: ParseOne[RawT, RowT],
    *,
    on_rejected: ParseFailureHandler[RawT] | None = None,
) -> tuple[list[BronzeParseResult[RowT]], list[RawT]]:
    """Parse many raw items, collecting exceptions as rejected raw payloads.

    A parser may return ``None`` to intentionally drop a raw item without
    counting it as rejected.
    """
    valid: list[BronzeParseResult[RowT]] = []
    rejected: list[RawT] = []
    for raw in raws:
        try:
            result = parse(raw)
        except Exception as exc:
            rejected.append(raw)
            if on_rejected:
                on_rejected(raw, exc)
            continue

        if result is not None:
            valid.append(result)
    return valid, rejected


def attach_source_uri[RowT: BronzeModel](
    results: Sequence[BronzeParseResult[RowT]],
    source_uri: str | S3ObjectRef,
) -> list[BronzeParseResult[RowT]]:
    """Attach one landing object URI to parser-produced results."""
    return [result.with_source_uri(source_uri) for result in results]


def _source_uri(source: str | S3ObjectRef) -> str:
    if isinstance(source, S3ObjectRef):
        return source.uri
    return source


__all__ = [
    "BronzeParser",
    "BronzeParseResult",
    "attach_source_uri",
    "parse_many",
    "parse_result",
    "parse_rows",
]
