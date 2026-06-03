"""Migration file naming and checksum helpers."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

_MIGRATION_FILE_RE = re.compile(r"^(?P<version>\d{14})_(?P<name>[a-z0-9_]+)\.sql$")


@dataclass(frozen=True, slots=True)
class MigrationFile:
    """One migration SQL file on disk."""

    version: str
    name: str
    path: Path
    checksum: str

    @property
    def filename(self) -> str:
        """Return the migration filename."""
        return self.path.name


def timestamp_version(now: datetime | None = None) -> str:
    """Return a UTC timestamp suitable for migration ordering."""
    resolved = now or datetime.now(UTC)
    return resolved.strftime("%Y%m%d%H%M%S")


def slugify_migration_name(name: str | None, *, default: str = "schema_diff") -> str:
    """Convert a human migration name into a filename slug."""
    raw = (name or default).strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    return slug or default


def migration_filename(*, version: str, name: str | None) -> str:
    """Return the canonical migration filename."""
    return f"{version}_{slugify_migration_name(name)}.sql"


def parse_migration_file(path: Path) -> MigrationFile:
    """Parse a migration path into metadata."""
    match = _MIGRATION_FILE_RE.match(path.name)
    if match is None:
        raise ValueError(f"Invalid migration filename: {path.name}")
    return MigrationFile(
        version=match.group("version"),
        name=match.group("name"),
        path=path,
        checksum=checksum_file(path),
    )


def list_migration_files(directory: Path) -> tuple[MigrationFile, ...]:
    """Return migration files sorted by version and filename."""
    if not directory.exists():
        return ()
    paths = sorted(path for path in directory.iterdir() if path.suffix == ".sql" and path.is_file())
    return tuple(parse_migration_file(path) for path in paths)


def checksum_file(path: Path) -> str:
    """Return the SHA-256 checksum for a migration file."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_migration_file(
    directory: Path,
    *,
    sql: str,
    name: str | None = None,
    version: str | None = None,
) -> Path:
    """Write a new migration file and return its path."""
    directory.mkdir(parents=True, exist_ok=True)
    resolved_version = version or timestamp_version()
    path = directory / migration_filename(version=resolved_version, name=name)
    if path.exists():
        raise FileExistsError(f"Migration already exists: {path}")
    path.write_text(sql, encoding="utf-8")
    return path


def empty_migration_sql(name: str | None = None) -> str:
    """Return a manual migration skeleton."""
    title = slugify_migration_name(name)
    return "\n".join(
        (
            "-- Manual lake schema migration.",
            f"-- Name: {title}",
            "-- Add SQL below. Review before applying to shared environments.",
            "",
        )
    )
