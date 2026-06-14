from __future__ import annotations

import csv
import io
import json
import pickle
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, Any, Literal, cast

if TYPE_CHECKING:
    from core.prefect.blocks import BlockEntry
from urllib.parse import urlparse

import boto3
import structlog
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from prefect_aws.s3 import S3Bucket

log = structlog.get_logger(__name__)

type S3DataFormat = Literal["bytes", "text", "json", "jsonl", "csv", "pickle"]

_MB = 1024 * 1024

_FORMAT_BY_SUFFIX: dict[str, S3DataFormat] = {
    ".bin": "bytes",
    ".csv": "csv",
    ".json": "json",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".pkl": "pickle",
    ".pickle": "pickle",
    ".txt": "text",
}

_CONTENT_TYPES: dict[S3DataFormat, str] = {
    "bytes": "application/octet-stream",
    "csv": "text/csv; charset=utf-8",
    "json": "application/json",
    "jsonl": "application/x-ndjson",
    "pickle": "application/octet-stream",
    "text": "text/plain; charset=utf-8",
}


@dataclass(frozen=True, slots=True)
class S3ObjectRef:
    """Reference to a stored S3 object."""

    bucket: str
    key: str

    @property
    def uri(self) -> str:
        """Return this object as an s3:// URI."""
        return f"s3://{self.bucket}/{self.key}"


class S3StorageClient:
    """Small S3 storage client for serialized objects and parallel uploads."""

    def __init__(
        self,
        *,
        bucket: str | None = None,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        client: Any | None = None,
        max_concurrency: int = 16,
        multipart_threshold: int = 8 * _MB,
        multipart_chunksize: int = 8 * _MB,
        default_extra_args: Mapping[str, Any] | None = None,
    ) -> None:
        """Create an S3 client using project settings as defaults."""
        self.bucket = bucket
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.max_concurrency = max_concurrency
        self.default_extra_args = dict(default_extra_args or {})

        self._client = client
        self._client_lock = Lock()
        self._transfer_config = TransferConfig(
            max_concurrency=max_concurrency,
            multipart_threshold=multipart_threshold,
            multipart_chunksize=multipart_chunksize,
            use_threads=True,
        )

    @property
    def client(self) -> Any:
        """Return the lazy boto3 S3 client."""
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    settings = self._settings()
                    client_kwargs: dict[str, Any] = {
                        "region_name": self.region_name or settings.aws_region,
                        "config": Config(
                            max_pool_connections=max(10, self.max_concurrency * 4),
                            retries={"max_attempts": 10, "mode": "adaptive"},
                            tcp_keepalive=True,
                        ),
                    }

                    if self.endpoint_url:
                        client_kwargs["endpoint_url"] = self.endpoint_url
                    if settings.aws_access_key_id and settings.aws_secret_access_key:
                        client_kwargs["aws_access_key_id"] = settings.aws_access_key_id
                        client_kwargs["aws_secret_access_key"] = settings.aws_secret_access_key

                    self._client = boto3.client("s3", **client_kwargs)

        return self._client

    def save(
        self,
        key: str,
        data: Any,
        *,
        format: S3DataFormat | None = None,
        bucket: str | None = None,
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
        extra_args: Mapping[str, Any] | None = None,
    ) -> S3ObjectRef:
        """Serialize data and upload it to S3."""
        resolved_bucket, resolved_key = self._bucket_key(key, bucket)
        resolved_format = self._format_for(resolved_key, data, format)
        body = self._serialize(data, resolved_format)

        self.client.upload_fileobj(
            io.BytesIO(body),
            resolved_bucket,
            resolved_key,
            ExtraArgs=self._extra_args(resolved_format, content_type, metadata, extra_args),
            Config=self._transfer_config,
        )
        log.info("s3.saved", bucket=resolved_bucket, key=resolved_key, bytes=len(body), format=resolved_format)
        return S3ObjectRef(bucket=resolved_bucket, key=resolved_key)

    def load(self, key: str, *, format: S3DataFormat | None = None, bucket: str | None = None) -> Any:
        """Download and deserialize an S3 object."""
        resolved_bucket, resolved_key = self._bucket_key(key, bucket)
        response = self.client.get_object(Bucket=resolved_bucket, Key=resolved_key)
        body = response["Body"].read()
        return self._deserialize(body, format or self._format_for(resolved_key, body, None))

    def upload_many(
        self,
        items: Mapping[str, Any] | Iterable[tuple[str, Any]],
        *,
        format: S3DataFormat | None = None,
        bucket: str | None = None,
        workers: int | None = None,
    ) -> list[S3ObjectRef]:
        """Upload many key/data pairs concurrently."""
        upload_items = list(items.items()) if isinstance(items, Mapping) else list(items)
        if not upload_items:
            return []

        def upload(item: tuple[str, Any]) -> S3ObjectRef:
            key, value = item
            return self.save(key, value, format=format, bucket=bucket)

        max_workers = max(1, min(workers or self.max_concurrency, len(upload_items)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(upload, upload_items))

    def exists(self, key: str, *, bucket: str | None = None) -> bool:
        """Return True if an S3 object exists."""
        resolved_bucket, resolved_key = self._bucket_key(key, bucket)
        try:
            self.client.head_object(Bucket=resolved_bucket, Key=resolved_key)
        except ClientError as exc:
            error = cast("dict[str, Any]", exc.response.get("Error", {}))
            if error.get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def delete(self, key: str, *, bucket: str | None = None) -> None:
        """Delete an object from S3. No-op if the object does not exist."""
        resolved_bucket, resolved_key = self._bucket_key(key, bucket)
        self.client.delete_object(Bucket=resolved_bucket, Key=resolved_key)
        log.info("s3.deleted", bucket=resolved_bucket, key=resolved_key)

    def list_keys(
        self,
        prefix: str = "",
        *,
        bucket: str | None = None,
        max_keys: int | None = None,
    ) -> list[str]:
        """Return all object keys under *prefix*, paginating automatically.

        Args:
            prefix: Key prefix to filter by (e.g. ``"bronze/price/"``).
            bucket: Override the default bucket.
            max_keys: Stop after collecting this many keys (``None`` = all).
        """
        resolved_bucket, resolved_prefix = self._bucket_key(prefix or "/", bucket)
        resolved_prefix = resolved_prefix.lstrip("/") if prefix else ""

        paginator = self.client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=resolved_bucket, Prefix=resolved_prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
                if max_keys is not None and len(keys) >= max_keys:
                    return keys
        return keys

    def _bucket_key(self, key: str, bucket: str | None) -> tuple[str, str]:
        if key.startswith("s3://"):
            parsed = urlparse(key)
            if not parsed.netloc or not parsed.path.strip("/"):
                raise ValueError(f"Invalid S3 URI: {key!r}")
            return parsed.netloc, parsed.path.lstrip("/")

        resolved_bucket = bucket or self.bucket
        if not resolved_bucket:
            raise ValueError("S3 bucket is required; pass bucket=... or set S3StorageClient(bucket=...)")
        return resolved_bucket, key.lstrip("/")

    def _extra_args(
        self,
        format: S3DataFormat,
        content_type: str | None,
        metadata: Mapping[str, str] | None,
        extra_args: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        args = dict(self.default_extra_args)
        args.update(extra_args or {})
        args.setdefault("ContentType", content_type or _CONTENT_TYPES[format])
        if metadata:
            args["Metadata"] = {str(key): str(value) for key, value in metadata.items()}
        return args

    def _format_for(self, key: str, data: Any, format: S3DataFormat | None) -> S3DataFormat:
        if format:
            return format

        suffix = Path(key).suffix.lower()
        if suffix in _FORMAT_BY_SUFFIX:
            return _FORMAT_BY_SUFFIX[suffix]
        if isinstance(data, bytes | bytearray | memoryview):
            return "bytes"
        if isinstance(data, str):
            return "text"
        return "json"

    def _serialize(self, data: Any, format: S3DataFormat) -> bytes:
        match format:
            case "bytes":
                if isinstance(data, bytes):
                    return data
                if isinstance(data, bytearray | memoryview):
                    return bytes(data)
                if isinstance(data, str):
                    return data.encode()
                raise TypeError(f"Cannot serialize {type(data).__name__} as bytes")
            case "text":
                return str(data).encode()
            case "json":
                return json.dumps(data, default=self._json_default, separators=(",", ":")).encode()
            case "jsonl":
                rows = self._rows(data)
                if not rows:
                    return b""
                return (
                    b"\n".join(
                        json.dumps(row, default=self._json_default, separators=(",", ":")).encode() for row in rows
                    )
                    + b"\n"
                )
            case "csv":
                return self._csv_dumps(data)
            case "pickle":
                return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)

    def _deserialize(self, data: bytes, format: S3DataFormat) -> Any:
        match format:
            case "bytes":
                return data
            case "text":
                return data.decode()
            case "json":
                return json.loads(data)
            case "jsonl":
                return [json.loads(line) for line in data.decode().splitlines() if line]
            case "csv":
                return list(csv.DictReader(io.StringIO(data.decode())))
            case "pickle":
                return pickle.loads(data)

    def _csv_dumps(self, data: Any) -> bytes:
        rows = self._rows(data)
        output = io.StringIO(newline="")

        if not rows:
            return b""
        if all(isinstance(row, Mapping) for row in rows):
            fieldnames = list(dict.fromkeys(str(key) for row in rows for key in row))
            writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows({str(key): value for key, value in row.items()} for row in rows)
        else:
            csv.writer(output).writerows(rows)

        return output.getvalue().encode()

    def _rows(self, data: Any) -> list[Any]:
        if isinstance(data, Mapping):
            return [data]
        if isinstance(data, Sequence) and not isinstance(data, str | bytes | bytearray | memoryview):
            return list(data)
        if isinstance(data, Iterable) and not isinstance(data, str | bytes | bytearray | memoryview):
            return list(data)
        raise TypeError(f"Expected a row mapping or iterable of rows, got {type(data).__name__}")

    @staticmethod
    def _json_default(value: Any) -> Any:
        if is_dataclass(value) and not isinstance(value, type):
            return asdict(value)

        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            return model_dump(mode="json")

        if isinstance(value, datetime | date):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, set):
            return sorted(value)
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    @classmethod
    def from_s3_bucket_block(
        cls,
        block: S3Bucket,
        **kwargs: Any,
    ) -> S3StorageClient:
        """Build a client from a loaded ``S3Bucket`` Prefect block.

        Credentials and bucket name are taken from the block so that tasks
        never read AWS secrets directly from settings.
        """
        max_concurrency: int = kwargs.get("max_concurrency", 16)
        session = block.credentials.get_boto3_session()
        boto_client = session.client(
            "s3",
            config=Config(
                max_pool_connections=max(10, max_concurrency * 4),
                retries={"max_attempts": 10, "mode": "adaptive"},
                tcp_keepalive=True,
            ),
        )
        return cls(bucket=block.bucket_name, client=boto_client, **kwargs)

    @classmethod
    async def from_block_entry(
        cls,
        entry: BlockEntry[S3Bucket],
        **kwargs: Any,
    ) -> S3StorageClient:
        """Load an ``S3Bucket`` block entry and build a client from it.

        Combines the ``BlockEntry.load_async`` and :meth:`from_s3_bucket_block`
        steps so callers can go straight from a registry entry to a ready client::

            s3 = await S3StorageClient.from_block_entry(BlockRegistry.S3_BUCKET)
        """
        if not isinstance(entry.block, S3Bucket):
            raise TypeError(f"Expected a BlockEntry[S3Bucket], got BlockEntry[{type(entry.block).__name__}]")
        block = await entry.load_async()
        return cls.from_s3_bucket_block(block, **kwargs)

    @staticmethod
    def _settings() -> Any:
        from config.settings import get_settings

        return get_settings()


@lru_cache(maxsize=1)
def get_s3_client() -> S3StorageClient:
    """Return the process-wide S3 storage client (local dev / tests only)."""
    return S3StorageClient()


def reset_s3_client() -> None:
    """Clear the process-wide S3 client cache."""
    get_s3_client.cache_clear()
