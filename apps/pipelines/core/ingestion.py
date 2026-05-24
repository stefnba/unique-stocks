"""Dataset-spec based ingestion helpers.

This module is the integration layer between provider models, domain parsers,
S3 landing storage, and Bronze lake tables.

The intended flow is:

1. A provider client returns strict ``ProviderModel`` instances.
2. The flow writes those provider-validated objects to S3 via ``save_landing``.
3. A domain parser converts provider objects into ``BronzeSource[Row]`` values.
4. ``write_bronze`` serializes the sources, adds ingestion metadata, checks the
   dataset policy, and inserts rows into the configured Bronze table.

The split keeps row models free of policy while giving domains a small,
consistent DX surface: define a ``LandingSpec``, define a ``BronzeDataset``,
and have parsers return ``BronzeSource``.
"""

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any, Literal

from pydantic import BaseModel

from core.clients.lake import DataLakeClient
from core.clients.storage.s3 import S3ObjectRef, S3StorageClient
from core.clients.storage.s3.keys import S3Domain, S3Key
from core.models import BronzeModel
from core.schema import BronzeTableModel

type LandingStyle = Literal["snapshot", "partitioned"]
type LandingFormat = Literal["json", "jsonl", "csv"]


@dataclass(frozen=True, slots=True)
class LandingSpec:
    """S3 landing-key policy for one provider payload shape.

    ``LandingSpec`` answers only one question: where should this provider
    payload be stored before parsing? It does not know the Bronze table shape.

    Use ``style="snapshot"`` for full-replacement reference snapshots such as
    exchanges. Use ``style="partitioned"`` when a provider payload is naturally
    scoped by values like exchange, ticker, date, or backfill range.

    ``partition_fields`` declares the domain-provided values required to build
    the key. ``include_ingested_at`` keeps repeated runs auditable without
    changing the logical partition policy.
    """

    s3_domain: S3Domain
    style: LandingStyle
    partition_fields: tuple[str, ...] = ()
    file_format: LandingFormat = "jsonl"
    include_ingested_at: bool = True

    def key(
        self,
        provider: str,
        *,
        ingested_at: datetime | date | str | None = None,
        **partitions: date | datetime | str | int,
    ) -> str:
        """Build the S3 key for this landing policy.

        ``partitions`` must provide every field named by ``partition_fields``.
        Values are serialized by the storage key builder into path-safe Hive
        partition segments.
        """
        stamp = ingested_at or datetime.now(UTC).replace(microsecond=0)
        if self.style == "snapshot":
            key = S3Key.snapshot(provider, self.s3_domain, ingested_at=stamp)
        else:
            missing = [field for field in self.partition_fields if field not in partitions]
            if missing:
                raise ValueError(f"Missing landing partition fields for {self.s3_domain}: {missing}")
            partition_values = {field: partitions[field] for field in self.partition_fields}
            if self.include_ingested_at:
                partition_values["ingested_at"] = stamp
            key = S3Key.partitioned_from_mapping(provider, self.s3_domain, partition_values)

        return getattr(key, self.file_format)()


@dataclass(frozen=True, slots=True)
class BronzeSource[RowT: BronzeModel]:
    """One parser output row plus its source-payload fragment.

    ``row`` is the normalized Bronze row model. ``raw_fragment`` is the
    smallest provider-side object responsible for that row and becomes
    ``raw_json``. For a 1:1 parse this is usually the provider item. For a 1:N
    parse, such as exchange holidays, it can be a child object plus parent
    context needed for lineage.

    ``source_uri`` is optional because parsers should stay pure; flows/tasks
    attach the S3 URI after the landing write succeeds.
    """

    row: RowT
    raw_fragment: Any
    source_uri: str | None = None

    def with_source_uri(self, source_uri: str) -> BronzeSource[RowT]:
        """Return this source with landing object lineage attached."""
        return BronzeSource(row=self.row, raw_fragment=self.raw_fragment, source_uri=source_uri)


@dataclass(frozen=True, slots=True)
class BronzeDataset[RowT: BronzeModel]:
    """Ingestion policy for one Bronze table.

    A dataset binds the provider, Bronze table, landing policy, and
    landing policy for a domain row type. The table spec owns the physical
    table name, schema, unique columns, and idempotency columns. It is the
    primary DX object for tasks: pass it to ``save_landing``,
    ``already_ingested``, and ``write_bronze`` instead of repeating
    provider/table/path details in every flow.

    Dataset specs are intentionally explicit and small. They do not generate
    Prefect flows; flows remain readable orchestration code.
    """

    provider: str
    table: type[BronzeTableModel]
    landing: LandingSpec

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

    def landing_key(
        self,
        *,
        ingested_at: datetime | date | str | None = None,
        **partitions: date | datetime | str | int,
    ) -> str:
        """Build a landing key using this dataset's provider and landing policy."""
        return self.landing.key(self.provider, ingested_at=ingested_at, **partitions)


def attach_source_uri[RowT: BronzeModel](
    sources: Sequence[BronzeSource[RowT]],
    source_uri: str,
) -> list[BronzeSource[RowT]]:
    """Attach one landing object URI to parser-produced sources.

    This is useful when a batch payload lands once but expands to many Bronze
    rows, such as an exchange instrument list or a schedule payload with many
    holiday rows.
    """
    return [source.with_source_uri(source_uri) for source in sources]


def save_landing(
    s3: S3StorageClient,
    spec: LandingSpec,
    provider: str,
    data: Any,
    *,
    ingested_at: datetime | date | str | None = None,
    **partitions: date | datetime | str | int,
) -> S3ObjectRef:
    """Save provider-validated data to S3 landing storage.

    The caller is expected to pass objects that have already passed provider
    model validation. Pydantic models are serialized with aliases so the landing
    object keeps provider field names rather than normalized Bronze names.
    """
    return s3.save(spec.key(provider, ingested_at=ingested_at, **partitions), _jsonable(data))


def bronze_record[RowT: BronzeModel](
    dataset: BronzeDataset[RowT],
    source: BronzeSource[RowT],
    *,
    source_uri: str | None = None,
) -> dict[str, Any]:
    """Serialize one ``BronzeSource`` into a row for ``lake.insert_rows``.

    The resulting dict contains the normalized Bronze payload plus the common
    ingestion envelope:

    - ``provider`` from the dataset spec
    - ``raw_json`` from the provider-side source fragment
    - ``row_hash`` from the normalized Bronze payload plus provider identity
    - ``source_uri`` from parser output or the task-level fallback
    """
    payload = source.row.to_payload()
    provider = str(dataset.provider)
    lineage_uri = source.source_uri or source_uri
    return {
        **payload,
        "provider": provider,
        "raw_json": canonical_json(source.raw_fragment),
        "row_hash": normalized_row_hash(payload, provider),
        "source_uri": lineage_uri,
    }


def bronze_records[RowT: BronzeModel](
    dataset: BronzeDataset[RowT],
    sources: Sequence[BronzeSource[RowT]],
    *,
    source_uri: str | None = None,
) -> list[dict[str, Any]]:
    """Serialize multiple Bronze sources into lake rows."""
    return [bronze_record(dataset, source, source_uri=source_uri) for source in sources]


def write_bronze[RowT: BronzeModel](
    lake: DataLakeClient,
    dataset: BronzeDataset[RowT],
    sources: Sequence[BronzeSource[RowT]],
    *,
    source_uri: str | None = None,
) -> int:
    """Insert Bronze sources into their configured lake table.

    This helper centralizes the last step of the ingestion contract. Tasks
    still decide when to call it and how to handle idempotency, partial success,
    and logging.
    """
    records = bronze_records(dataset, sources, source_uri=source_uri)
    if not records:
        return 0
    return lake.insert_rows(dataset.schema, dataset.table_name, records)


def already_ingested[RowT: BronzeModel](
    lake: DataLakeClient,
    dataset: BronzeDataset[RowT],
    **values: date | str | int,
) -> bool:
    """Return True when rows already exist for a dataset idempotency partition.

    ``values`` must include every column listed in
    ``dataset.idempotency_columns``. Provider is always included automatically
    because the same Bronze table may later accept rows from multiple sources.
    """
    missing = [column for column in dataset.idempotency_columns if column not in values]
    if missing:
        raise ValueError(f"Missing idempotency values for {dataset.table_name}: {missing}")

    qualified = lake.qualified_name(dataset.schema, dataset.table_name)
    clauses = [f"{column} = ?" for column in dataset.idempotency_columns]
    clauses.append("provider = ?")
    params = [_sql_value(values[column]) for column in dataset.idempotency_columns]
    params.append(str(dataset.provider))
    row = lake.query_one(
        f"SELECT COUNT(*) AS cnt FROM {qualified} WHERE {' AND '.join(clauses)}",
        params,
    )
    return bool(row and row["cnt"] > 0)


def canonical_json(value: Any) -> str:
    """Return deterministic compact JSON for provider or Bronze payloads.

    Pydantic models are dumped with provider aliases. That means provider
    fragments stored in ``raw_json`` keep source field names, while normalized
    Bronze hashes remain stable because ``bronze_record`` passes row payload
    dicts that already use Bronze field names.
    """
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"))


def normalized_row_hash(payload: Mapping[str, Any], provider: str) -> str:
    """Return a stable hash for normalized Bronze payload plus provider identity.

    ``raw_json`` is intentionally excluded. The hash tracks the normalized row
    we would insert into DuckDB, not incidental source-payload formatting.
    """
    return sha256(canonical_json({**payload, "provider": provider}).encode()).hexdigest()


def _sql_value(value: date | str | int) -> str | int:
    if isinstance(value, date):
        return value.isoformat()
    return value


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", by_alias=True)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray | memoryview):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value
