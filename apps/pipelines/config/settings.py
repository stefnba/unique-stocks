"""Application settings loaded from environment variables and .env files."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

type Environment = Literal["dev", "prod", "docker_dev"]


class Settings(BaseSettings):
    """Runtime configuration for the pipelines app.

    All values can be overridden via environment variables or a ``.env`` file.
    Secrets are stored as ``SecretStr`` to prevent accidental logging.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Data provider
    eodhd_api_key: SecretStr = Field(default=SecretStr(""), description="API key for EODHD.")
    motherduck_token: SecretStr = Field(
        default=SecretStr(""),
        description="MotherDuck token. When blank, lake.py falls back to the local DuckDB file.",
    )
    local_lake_path: str = Field(
        default="unique_stocks.duckdb",
        description="Local DuckDB file path used when MOTHERDUCK_TOKEN is blank.",
    )

    # AWS credentials (used only in config/blocks.py to bootstrap the S3_BUCKET block)
    aws_access_key_id: str = Field(default="", description="Access key ID for AWS.")
    aws_secret_access_key: SecretStr = Field(default=SecretStr(""), description="Secret access key for AWS.")

    # Prefect
    prefect_api_url: str = "http://127.0.0.1:4200/api"
    prefect_api_key: SecretStr = Field(default=SecretStr(""), description="API key for Prefect.")

    # Environment
    environment: Environment = "dev"

    @property
    def is_production(self) -> bool:
        """Return ``True`` when running in the production environment."""
        return self.environment == "prod"


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
