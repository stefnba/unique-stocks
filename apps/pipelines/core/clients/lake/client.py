from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from threading import RLock
from typing import Any, Literal, cast

import duckdb
import structlog

log = structlog.get_logger(__name__)

type LakeDataFormat = Literal["rows", "csv", "json", "jsonl", "parquet"]
type LakeWriteMode = Literal["append", "replace", "create"]
type DuckDBConfigValue = str | bool | int | float | list[str]

_DEFAULT_SCHEMAS = ("bronze", "silver", "gold", "pipeline")
_FORMAT_BY_SUFFIX: dict[str, LakeDataFormat] = {
    ".csv": "csv",
    ".json": "json",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".parquet": "parquet",
}


@dataclass(slots=True, frozen=True)
class LakeUpload:
    """One data lake write with optional per-table settings.

    Leave ``format`` and ``mode`` as ``None`` to inherit the caller's defaults
    in :meth:`DataLakeClient.save_many`.
    """

    table: str
    data: Any
    schema: str = "bronze"
    format: LakeDataFormat | None = None
    mode: LakeWriteMode | None = None


class DataLakeClient:
    """Client to save and retrieve data from DuckDB or MotherDuck."""

    def __init__(
        self,
        *,
        connection_string: str | None = None,
        connection: duckdb.DuckDBPyConnection | None = None,
        schemas: Sequence[str] = _DEFAULT_SCHEMAS,
        read_only: bool = False,
        parallel_workers: int = 8,
        threads: int | None = None,
        config: Mapping[str, DuckDBConfigValue] | None = None,
    ) -> None:
        """Create a reusable data lake client."""
        self.connection_string = connection_string
        self.schemas = tuple(schemas)
        self.read_only = read_only
        self.parallel_workers = parallel_workers
        self.threads = threads
        self.config = dict(config or {})

        self._conn = connection
        self._lock = RLock()
        if self._conn is not None and not self.read_only:
            self._ensure_schemas(self._conn)

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Return the lazy DuckDB/MotherDuck connection."""
        if self._conn is None:
            with self._lock:
                if self._conn is None:
                    self._conn = self._connect()
        return self._conn

    def close(self) -> None:
        """Close the cached connection."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """Run a write statement with no return value."""
        with self._lock:
            self.connection.execute(sql, list(params or []))

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Run a SELECT and return rows as dictionaries."""
        with self._lock:
            result = self.connection.execute(sql, list(params or []))
            columns = [description[0] for description in result.description]
            return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        """Run a SELECT and return the first row as a dictionary."""
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def table_exists(self, schema: str, table: str) -> bool:
        """Return True when a table exists."""
        row = self.query_one(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        )
        return row is not None

    def save(
        self,
        table: str,
        data: Any,
        *,
        schema: str = "bronze",
        format: LakeDataFormat | None = None,
        mode: LakeWriteMode = "append",
    ) -> int:
        """Save rows or a supported data file into a lake table."""
        resolved_format = self._infer_format(data, format)
        with self._lock:
            if resolved_format == "rows":
                return self._save_rows(schema, table, data, mode)
            return self._save_file(schema, table, data, resolved_format, mode)

    def save_many(
        self,
        items: Iterable[LakeUpload | Mapping[str, Any] | tuple[str, Any]],
        *,
        schema: str = "bronze",
        format: LakeDataFormat | None = None,
        mode: LakeWriteMode = "append",
        workers: int | None = None,
    ) -> list[int]:
        """Save many tables concurrently while preserving input order."""
        from concurrent.futures import ThreadPoolExecutor

        uploads = list(self._coerce_uploads(items, schema=schema, format=format, mode=mode))
        if not uploads:
            return []

        if self._conn is not None and self.connection_string is None:
            return [
                self.save(
                    upload.table,
                    upload.data,
                    schema=upload.schema,
                    format=upload.format,
                    mode=upload.mode or "append",
                )
                for upload in uploads
            ]

        max_workers = max(1, min(workers or self.parallel_workers, len(uploads)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(self._save_upload_in_worker, uploads))

    def upload_many(
        self,
        items: Iterable[LakeUpload | Mapping[str, Any] | tuple[str, Any]],
        *,
        schema: str = "bronze",
        format: LakeDataFormat | None = None,
        mode: LakeWriteMode = "append",
        workers: int | None = None,
    ) -> list[int]:
        """Alias for save_many for callers that use upload terminology."""
        return self.save_many(items, schema=schema, format=format, mode=mode, workers=workers)

    def load(
        self,
        table: str,
        *,
        schema: str = "bronze",
        columns: Sequence[str] | None = None,
        where: str | None = None,
        params: Sequence[Any] | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Load a table or filtered table slice as dictionaries."""
        selected = ", ".join(self._quote_identifier(column) for column in columns) if columns else "*"
        sql = f"SELECT {selected} FROM {self._qualified(schema, table)}"
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        if limit is not None:
            sql += f" LIMIT {max(0, int(limit))}"
        return self.query(sql, params)

    def insert_rows(self, schema: str, table: str, rows: Iterable[Mapping[str, Any]]) -> int:
        """Bulk-insert dictionary rows into schema.table."""
        return self.save(table, rows, schema=schema, format="rows", mode="append")

    def _connect(self) -> duckdb.DuckDBPyConnection:
        settings = self._settings()
        if self.connection_string:
            conn_str = self.connection_string
        elif settings.motherduck_token.get_secret_value():
            conn_str = f"md:unique_stocks?motherduck_token={settings.motherduck_token.get_secret_value()}"
        else:
            conn_str = "unique_stocks.db"
        log.info("lake.connecting", connection=conn_str.split("?")[0])
        if self.config:
            conn = duckdb.connect(conn_str, self.read_only, self.config)
        else:
            conn = duckdb.connect(conn_str, self.read_only)
        if self.threads is not None:
            conn.execute("SET threads TO ?", [max(1, int(self.threads))])
        if not self.read_only:
            self._ensure_schemas(conn)
        return conn

    def _settings(self) -> Any:
        from config.settings import get_settings

        return get_settings()

    def _ensure_schemas(self, conn: duckdb.DuckDBPyConnection) -> None:
        for schema in self.schemas:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._quote_identifier(schema)}")

    def _save_rows(self, schema: str, table: str, data: Any, mode: LakeWriteMode) -> int:
        rows = [self._coerce_row(row) for row in self._iter_rows(data)]
        if not rows:
            if mode == "replace" and self._table_exists(schema, table):
                self.connection.execute(f"DELETE FROM {self._qualified(schema, table)}")
            return 0

        qualified = self._qualified(schema, table)
        exists = self._table_exists(schema, table)
        if mode == "replace":
            self.connection.execute(f"DROP TABLE IF EXISTS {qualified}")
            self._create_table_for_rows(schema, table, rows)
        elif mode == "create" or not exists:
            self._create_table_for_rows(schema, table, rows)

        columns = list(rows[0].keys())
        col_names = ", ".join(self._quote_identifier(column) for column in columns)
        placeholders = ", ".join("?" for _ in columns)
        values = [[self._adapt_value(row[column]) for column in columns] for row in rows]
        self.connection.executemany(f"INSERT INTO {qualified} ({col_names}) VALUES ({placeholders})", values)
        log.info("lake.rows_saved", schema=schema, table=table, rows=len(rows), mode=mode)
        return len(rows)

    def _save_file(self, schema: str, table: str, data: Any, format: LakeDataFormat, mode: LakeWriteMode) -> int:
        path = Path(data)
        source_sql = {
            "csv": f"SELECT * FROM read_csv_auto({self._sql_literal(path)})",
            "json": f"SELECT * FROM read_json_auto({self._sql_literal(path)})",
            "jsonl": f"SELECT * FROM read_json_auto({self._sql_literal(path)})",
            "parquet": f"SELECT * FROM read_parquet({self._sql_literal(path)})",
        }.get(format)
        if source_sql is None:
            raise ValueError(f"Unsupported file format for lake save: {format!r}")

        count = self._write_select(schema, table, source_sql, mode)
        log.info("lake.file_saved", schema=schema, table=table, path=str(path), rows=count, format=format, mode=mode)
        return count

    def _write_select(self, schema: str, table: str, source_sql: str, mode: LakeWriteMode) -> int:
        qualified = self._qualified(schema, table)
        exists = self._table_exists(schema, table)

        if mode == "append" and exists:
            result = self.connection.execute(f"INSERT INTO {qualified} {source_sql}")
            return self._statement_row_count(result) or self._source_row_count(source_sql)
        if mode == "replace":
            result = self.connection.execute(f"CREATE OR REPLACE TABLE {qualified} AS {source_sql}")
        else:
            result = self.connection.execute(f"CREATE TABLE {qualified} AS {source_sql}")
        return self._statement_row_count(result) or self._table_row_count(schema, table)

    def _save_upload_in_worker(self, upload: LakeUpload) -> int:
        client = DataLakeClient(
            connection_string=self.connection_string,
            schemas=self.schemas,
            read_only=self.read_only,
            parallel_workers=self.parallel_workers,
            threads=self.threads,
            config=self.config,
        )
        try:
            return client.save(
                upload.table,
                upload.data,
                schema=upload.schema,
                format=upload.format,
                mode=upload.mode or "append",
            )
        finally:
            client.close()

    def _coerce_uploads(
        self,
        items: Iterable[LakeUpload | Mapping[str, Any] | tuple[str, Any]],
        *,
        schema: str,
        format: LakeDataFormat | None,
        mode: LakeWriteMode,
    ) -> Iterable[LakeUpload]:
        for item in items:
            if isinstance(item, LakeUpload):
                yield LakeUpload(item.table, item.data, item.schema, item.format or format, item.mode or mode)
                continue
            if isinstance(item, tuple):
                table, data = item
                yield LakeUpload(table, data, schema, format, mode)
                continue

            upload = cast(Mapping[str, Any], item)
            yield LakeUpload(
                table=str(upload["table"]),
                data=upload["data"],
                schema=str(upload.get("schema", schema)),
                format=upload.get("format", format),
                mode=upload.get("mode", mode),
            )

    def _infer_format(self, data: Any, format: LakeDataFormat | None) -> LakeDataFormat:
        if format is not None:
            return format
        if isinstance(data, str | Path):
            suffix = Path(data).suffix.lower()
            if suffix in _FORMAT_BY_SUFFIX:
                return _FORMAT_BY_SUFFIX[suffix]
        return "rows"

    def _iter_rows(self, data: Any) -> Iterable[Any]:
        if isinstance(data, Mapping) or self._is_model(data):
            yield data
            return
        if isinstance(data, str | bytes | bytearray | memoryview):
            raise TypeError("String and byte data cannot be saved as rows; pass a path or explicit format.")
        yield from data

    def _coerce_row(self, row: Any) -> dict[str, Any]:
        if isinstance(row, Mapping):
            return dict(row)
        if self._is_model(row):
            return dict(row.model_dump(mode="json"))
        raise TypeError(f"Cannot save {type(row).__name__} as a lake row")

    def _adapt_value(self, value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, Mapping | list | tuple):
            return json.dumps(value, default=self._json_default, separators=(",", ":"))
        return value

    def _create_table_for_rows(self, schema: str, table: str, rows: Sequence[Mapping[str, Any]]) -> None:
        columns = list(rows[0].keys())
        if not columns:
            raise ValueError("Cannot create a lake table from rows with no columns")
        definitions = ", ".join(
            f"{self._quote_identifier(column)} {self._infer_sql_type(row.get(column) for row in rows)}"
            for column in columns
        )
        self.connection.execute(f"CREATE TABLE {self._qualified(schema, table)} ({definitions})")

    def _infer_sql_type(self, values: Iterable[Any]) -> str:
        for value in values:
            if value is None:
                continue
            if isinstance(value, bool):
                return "BOOLEAN"
            if isinstance(value, int):
                return "BIGINT"
            if isinstance(value, float):
                return "DOUBLE"
            if isinstance(value, Decimal):
                return "DECIMAL(38, 10)"
            if isinstance(value, datetime):
                return "TIMESTAMPTZ"
            if isinstance(value, date):
                return "DATE"
            if isinstance(value, Mapping | list | tuple):
                return "JSON"
            return "VARCHAR"
        return "VARCHAR"

    def _table_exists(self, schema: str, table: str) -> bool:
        return (
            self.connection.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                [schema, table],
            ).fetchone()
            is not None
        )

    def _table_row_count(self, schema: str, table: str) -> int:
        row = self.connection.execute(f"SELECT COUNT(*) FROM {self._qualified(schema, table)}").fetchone()
        return int(row[0]) if row is not None else 0

    def _source_row_count(self, source_sql: str) -> int:
        row = self.connection.execute(f"SELECT COUNT(*) FROM ({source_sql}) AS source").fetchone()
        return int(row[0]) if row is not None else 0

    def _statement_row_count(self, result: Any) -> int | None:
        rows = result.fetchall()
        if len(rows) == 1 and len(rows[0]) == 1 and isinstance(rows[0][0], int):
            return rows[0][0]
        return None

    def _json_default(self, value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, date | datetime):
            return value.isoformat()
        if self._is_model(value):
            return value.model_dump(mode="json")
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    def _is_model(self, value: Any) -> bool:
        model_dump = getattr(value, "model_dump", None)
        return callable(model_dump)

    def qualified_name(self, schema: str, table: str) -> str:
        """Return a fully-qualified, safely-quoted ``schema.table`` identifier."""
        return f"{self._quote_identifier(schema)}.{self._quote_identifier(table)}"

    def _qualified(self, schema: str, table: str) -> str:
        return self.qualified_name(schema, table)

    def _quote_identifier(self, value: str) -> str:
        if not value or "\x00" in value:
            raise ValueError(f"Invalid DuckDB identifier: {value!r}")
        return '"' + value.replace('"', '""') + '"'

    def _sql_literal(self, value: str | Path) -> str:
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"


@lru_cache(maxsize=1)
def get_lake_client() -> DataLakeClient:
    """Return the process-wide data lake client."""
    return DataLakeClient()


def reset_lake_client() -> None:
    """Close and clear the process-wide data lake client."""
    client = get_lake_client()
    client.close()
    get_lake_client.cache_clear()
