"""Application settings loaded from environment variables and .env files."""

from functools import lru_cache
from pathlib import Path
from typing import Final, Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

type Environment = Literal["dev", "prod", "docker_dev"]
type LakeBackend = Literal["local", "motherduck"]
type DbtTarget = Literal["dev", "prod"]
type PipelineLogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]
type PipelineLogFormat = Literal["auto", "console", "json"]

APP_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
_DEFAULT_LAKE_NAME: Final[str] = "unique_stocks"
DEFAULT_PREFECT_LAKE_WRITER_LIMIT: Final[int] = 1
PROD_MOTHERDUCK_ERROR: Final[str] = (
    "ENVIRONMENT=prod requires MOTHERDUCK_TOKEN; set MOTHERDUCK_TOKEN or use ENVIRONMENT=dev"
)


class Settings(BaseSettings):
    """Runtime configuration for the pipelines app.

    All values can be overridden via environment variables or a ``.env`` file.
    Secrets are stored as ``SecretStr`` to prevent accidental logging.
    """

    model_config = SettingsConfigDict(env_file=str(APP_ROOT / ".env"), env_file_encoding="utf-8", extra="ignore")

    # Data provider
    eodhd_api_key: SecretStr = Field(default=SecretStr(""), description="API key for EODHD.")
    motherduck_token: SecretStr = Field(
        default=SecretStr(""),
        description="MotherDuck token. When blank, lake.py falls back to the local DuckDB file.",
    )
    lake_name: str = Field(
        default=_DEFAULT_LAKE_NAME,
        description="Logical lake name; shared with dbt via LAKE_NAME and used as the MotherDuck database name.",
    )
    local_lake_path: str = Field(
        default="",
        description="Optional local DuckDB file path when MOTHERDUCK_TOKEN is blank (LOCAL_LAKE_PATH).",
    )

    # AWS credentials (used only in config/blocks.py to bootstrap the S3_BUCKET block)
    aws_access_key_id: str = Field(default="", description="Access key ID for AWS.")
    aws_secret_access_key: SecretStr = Field(default=SecretStr(""), description="Secret access key for AWS.")

    # Prefect
    prefect_api_url: str = "http://127.0.0.1:4200/api"
    prefect_api_key: SecretStr = Field(default=SecretStr(""), description="API key for Prefect.")
    prefect_lake_writer_limit: int = Field(
        default=DEFAULT_PREFECT_LAKE_WRITER_LIMIT,
        ge=1,
        description="Prefect global concurrency slots for shared lake/dbt writes.",
    )

    # Environment
    environment: Environment = "dev"

    # Logging
    pipeline_log_level: PipelineLogLevel = "INFO"
    pipeline_log_format: PipelineLogFormat = "auto"

    @property
    def is_production(self) -> bool:
        """Return ``True`` when running in the production environment."""
        return self.environment == "prod"

    @property
    def motherduck_database_name(self) -> str:
        """Return the provider-specific MotherDuck database name for this lake."""
        return self.lake_name

    @model_validator(mode="after")
    def validate_environment(self) -> Settings:
        """Reject unsafe production settings before any runtime work starts."""
        if self.is_production and self.lake_backend() != "motherduck":
            raise ValueError(PROD_MOTHERDUCK_ERROR)
        return self

    def lake_backend(self) -> LakeBackend:
        """Return the active lake backend selected by runtime credentials."""
        if self.motherduck_token.get_secret_value().strip():
            return "motherduck"
        return "local"

    def resolved_local_lake_path(self) -> str:
        """Return an absolute DuckDB path so workers and CLI share the same lake file."""
        path = Path(self._local_lake_path())
        if path.is_absolute():
            return str(path)
        return str((APP_ROOT / path).resolve())

    def _local_lake_path(self) -> str:
        """Return configured local path or derive one from the lake name."""
        configured_path = self.local_lake_path.strip()
        if configured_path:
            return configured_path
        return f"{self.lake_name}.duckdb"

    def resolved_dbt_target(self) -> DbtTarget:
        """Return the dbt target matching the selected lake backend."""
        if self.lake_backend() == "motherduck":
            return "prod"
        return "dev"

    def dbt_env_overlay(self) -> dict[str, str]:
        """Return env vars that keep dbt CLI and Python lake writes aligned."""
        target = self.resolved_dbt_target()
        overlay = {
            "DBT_TARGET": target,
            "LAKE_NAME": self.lake_name,
            "LOCAL_LAKE_PATH": self.resolved_local_lake_path(),
        }
        if target == "dev":
            overlay["DBT_DUCKDB_PATH"] = self.resolved_local_lake_path()
        else:
            overlay["MOTHERDUCK_TOKEN"] = self.motherduck_token.get_secret_value().strip()
        return overlay


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached ``Settings`` singleton.

    Only use this in core client internals (lazy import to avoid circular
    imports) and in tests that need to swap settings between cases::

        get_settings.cache_clear()
        monkeypatch.setenv("ENVIRONMENT", "prod")

    Tasks and flows must load credentials from ``BlockRegistry``, not settings.
    """
    return Settings()


SETTINGS = get_settings()
"""Process-wide settings singleton.

Only use this in ``config/blocks.py`` to build the initial block registry.
Tasks and flows load credentials via ``BlockRegistry`` at runtime.
"""
