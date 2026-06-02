"""Bronze dataset metadata and write helpers for ingestion domains."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass, replace
from datetime import date
from hashlib import sha256
from typing import Any

from core.clients.lake import DataLakeClient
from core.clients.storage.s3.base import S3ObjectRef
from core.ingestion.landing import LandingTargetBase
from core.ingestion.parser import BronzeParseResult
from core.ingestion.serialization import canonical_json, sql_value
from core.models import BronzeModel
from core.schema import BronzeTableModel


@dataclass(frozen=True, slots=True)
class BronzeWrite:
    """Structured result from a domain Bronze write task.

    Attributes:
        rows_written: Number of rows inserted into Bronze.
        reason: Optional reason when the write intentionally produced no new rows.
    """

    rows_written: int
    reason: str | None = None

    @property
    def skipped(self) -> bool:
        """Return True when the write intentionally produced no new rows."""
        return self.rows_written == 0 and self.reason is not None


@dataclass(frozen=True, slots=True)
class BronzeDataset[LandingsT = object]:
    """Bronze lake target for normalized ingestion rows.

    Attributes:
        provider: Source provider identifier stamped into Bronze rows.
        table: Bronze table spec that owns schema, row model, and keys.
        landings: Typed landing target group for raw payloads that feed this dataset.
    """

    provider: str
    table: type[BronzeTableModel]
    landings: LandingsT

    def __post_init__(self) -> None:
        """Bind default audit dataset labels from landing group field names."""
        if not is_dataclass(self.landings) or isinstance(self.landings, type):
            return

        updates = {}
        for field in fields(self.landings):
            target = getattr(self.landings, field.name)
            if isinstance(target, LandingTargetBase) and target.audit_dataset is None:
                updates[field.name] = replace(target, audit_dataset=f"{target.domain}.{field.name}")
        if updates:
            object.__setattr__(self, "landings", replace(self.landings, **updates))

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
        """Serialize one parsed Bronze row into a lake row.

        Args:
            source: Parsed row with its raw provider fragment.
            source_ref: Optional landing object reference for lineage.
            source_uri: Optional landing object URI for lineage.

        Returns:
            Lake-ready row with provider, raw JSON, row hash, and source URI.

        Raises:
            TypeError: If ``source.row`` does not match this dataset's row model.
        """
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
        """Insert parsed Bronze rows into this dataset's lake table.

        Args:
            lake: Lake client used for insertion.
            sources: Parsed rows and raw fragments to write.
            source_ref: Optional landing object reference applied to every row.
            source_uri: Optional landing object URI applied to every row.

        Returns:
            Number of inserted rows.
        """
        records = [self.bronze_record(source, source_ref=source_ref, source_uri=source_uri) for source in sources]
        if not records:
            return 0
        return lake.insert_rows(self.schema, self.table_name, records)

    def already_ingested(self, lake: DataLakeClient, **values: date | str | int) -> bool:
        """Return True when this dataset's idempotency partition already exists.

        Args:
            lake: Lake client used for the idempotency query.
            **values: Values for every configured idempotency column.

        Returns:
            True when at least one provider row exists for the idempotency key.

        Raises:
            ValueError: If a required idempotency value is missing.
        """
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
