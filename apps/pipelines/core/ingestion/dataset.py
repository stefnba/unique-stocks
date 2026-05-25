from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from typing import Any

from core.clients.lake import DataLakeClient
from core.clients.storage.s3.base import S3ObjectRef
from core.ingestion.parser import BronzeParseResult, attach_source_uri
from core.ingestion.serialization import canonical_json
from core.models import BronzeModel
from core.schema import BronzeTableModel


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
            "row_hash": normalized_row_hash(payload, data_provider),
            "source_uri": lineage_uri,
        }

    def bronze_records[RowT: BronzeModel](
        self,
        sources: Sequence[BronzeParseResult[RowT]],
        *,
        source_ref: S3ObjectRef | None = None,
        source_uri: str | None = None,
    ) -> list[dict[str, Any]]:
        """Serialize multiple parsed Bronze rows into lake rows."""
        return [self.bronze_record(source, source_ref=source_ref, source_uri=source_uri) for source in sources]

    def write_bronze[RowT: BronzeModel](
        self,
        lake: DataLakeClient,
        sources: Sequence[BronzeParseResult[RowT]],
        *,
        source_ref: S3ObjectRef | None = None,
        source_uri: str | None = None,
    ) -> int:
        """Insert parsed Bronze rows into this dataset's lake table."""
        records = self.bronze_records(sources, source_ref=source_ref, source_uri=source_uri)
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
        params = [_sql_value(values[column]) for column in self.idempotency_columns]
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


def bronze_record[RowT: BronzeModel](
    dataset: BronzeDataset[Any],
    source: BronzeParseResult[RowT],
    *,
    source_ref: S3ObjectRef | None = None,
    source_uri: str | None = None,
) -> dict[str, Any]:
    """Serialize one parsed Bronze row into a lake row."""
    return dataset.bronze_record(source, source_ref=source_ref, source_uri=source_uri)


def bronze_records[RowT: BronzeModel](
    dataset: BronzeDataset[Any],
    sources: Sequence[BronzeParseResult[RowT]],
    *,
    source_ref: S3ObjectRef | None = None,
    source_uri: str | None = None,
) -> list[dict[str, Any]]:
    """Serialize multiple parsed Bronze rows into lake rows."""
    return dataset.bronze_records(sources, source_ref=source_ref, source_uri=source_uri)


def write_bronze[RowT: BronzeModel](
    lake: DataLakeClient,
    dataset: BronzeDataset[Any],
    sources: Sequence[BronzeParseResult[RowT]],
    *,
    source_ref: S3ObjectRef | None = None,
    source_uri: str | None = None,
) -> int:
    """Insert parsed Bronze rows into their configured lake table."""
    return dataset.write_bronze(lake, sources, source_ref=source_ref, source_uri=source_uri)


def already_ingested[RowT: BronzeModel](
    lake: DataLakeClient,
    dataset: BronzeDataset[Any],
    **values: date | str | int,
) -> bool:
    """Return True when rows already exist for a dataset idempotency partition."""
    return dataset.already_ingested(lake, **values)


def normalized_row_hash(payload: Mapping[str, Any], data_provider: str) -> str:
    """Return a stable hash for normalized Bronze payload plus provider identity."""
    return sha256(canonical_json({**payload, "data_provider": data_provider}).encode()).hexdigest()


def _sql_value(value: date | str | int) -> str | int:
    if isinstance(value, date):
        return value.isoformat()
    return value


__all__ = [
    "BronzeDataset",
    "BronzeParseResult",
    "already_ingested",
    "attach_source_uri",
    "bronze_record",
    "bronze_records",
    "canonical_json",
    "normalized_row_hash",
    "write_bronze",
]
