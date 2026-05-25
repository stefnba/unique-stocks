"""Parser-facing ingestion types."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from core.clients.storage.s3.base import S3ObjectRef
from core.models import BronzeModel


@dataclass(frozen=True, slots=True)
class BronzeParseResult[RowT: BronzeModel]:
    """One parsed Bronze row plus the raw provider fragment that produced it."""

    row: RowT
    raw_fragment: Any
    source_uri: str | None = None

    def with_source_uri(self, source_uri: str | S3ObjectRef) -> BronzeParseResult[RowT]:
        """Return this parse result with landing object lineage attached."""
        return BronzeParseResult(row=self.row, raw_fragment=self.raw_fragment, source_uri=_source_uri(source_uri))


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
    "BronzeParseResult",
    "attach_source_uri",
]
