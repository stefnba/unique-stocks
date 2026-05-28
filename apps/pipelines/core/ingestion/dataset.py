from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from typing import Any

from core.clients.lake import DataLakeClient
from core.clients.storage.s3.base import S3ObjectRef
from core.ingestion.parser import BronzeParseResult
from core.ingestion.serialization import canonical_json, sql_value
from core.models import BronzeModel
from core.schema import BronzeTableModel


@dataclass(frozen=True, slots=True)
class BronzeWrite:
    """Structured result from a domain Bronze write task."""

    rows_written: int
    reason: str | None = None

    @property
    def skipped(self) -> bool:
        """Return True when the write intentionally produced no new rows."""
        return self.rows_written == 0 and self.reason is not None


@dataclass(frozen=True, slots=True)
class BronzeDataset[LandingsT = object]:
    """Bronze lake target for normalized ingestion rows."""

    provider: str
    table: type[BronzeTableModel]
    landings: LandingsT

    @property
    def schema(self) -> str:
        """Return the lake schema name from the table spec."""
        return self.table.schema_name

    @property
    def table_name(self) -> str:
        """Return the physical table name from the table spec."""
        return self.table.table_name

    @property
    def idempotency_columns(self) -> tuple[str, ...]:
        """Return idempotency columns from the table spec."""
        return self.table.idempotency_column_names()

    def bronze_record[RowT: BronzeModel](
        self,
        source: BronzeParseResult[RowT],
        *,
        source_ref: S3ObjectRef | None = None,
        source_uri: str | None = None,
    ) -> dict[str, Any]:
        """Serialize one parsed Bronze row into a lake row."""
        self._validate_source_row(source)
        payload = source.row.to_payload()
        data_provider = str(self.provider)
        lineage_uri = source.source_uri or source_uri or (source_ref.uri if source_ref else None)
        return {
            **payload,
            "data_provider": data_provider,
            "raw_json": canonical_json(source.raw_fragment),
            "row_hash": _normalized_row_hash(payload, data_provider),
            "source_uri": lineage_uri,
        }

    def write_bronze[RowT: BronzeModel](
        self,
        lake: DataLakeClient,
        sources: Sequence[BronzeParseResult[RowT]],
        *,
        source_ref: S3ObjectRef | None = None,
        source_uri: str | None = None,
    ) -> int:
        """Insert parsed Bronze rows into this dataset's lake table."""
        records = [self.bronze_record(source, source_ref=source_ref, source_uri=source_uri) for source in sources]
        if not records:
            return 0
        return lake.insert_rows(self.schema, self.table_name, records)

    def already_ingested(self, lake: DataLakeClient, **values: date | str | int) -> bool:
        """Return True when this dataset's idempotency partition already exists."""
        missing = [column for column in self.idempotency_columns if column not in values]
        if missing:
            raise ValueError(f"Missing idempotency values for {self.table_name}: {missing}")

        qualified = lake.qualified_name(self.schema, self.table_name)
        clauses = [f"{column} = ?" for column in self.idempotency_columns]
        clauses.append("data_provider = ?")
        params = [sql_value(values[column]) for column in self.idempotency_columns]
        params.append(str(self.provider))
        row = lake.query_one(
            f"SELECT COUNT(*) AS cnt FROM {qualified} WHERE {' AND '.join(clauses)}",
            params,
        )
        return bool(row and row["cnt"] > 0)

    def _validate_source_row(self, source: BronzeParseResult[Any]) -> None:
        expected = self.table.row_model
        if not isinstance(source.row, expected):
            raise TypeError(
                f"{self.table_name} expects BronzeParseResult row {expected.__name__}, got {type(source.row).__name__}"
            )


def _normalized_row_hash(payload: Mapping[str, Any], data_provider: str) -> str:
    """Return a stable hash for normalized Bronze payload plus provider identity."""
    return sha256(canonical_json({**payload, "data_provider": data_provider}).encode()).hexdigest()


__all__ = [
    "BronzeDataset",
    "BronzeWrite",
]
