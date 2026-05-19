from functools import lru_cache
from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

type Environment = Literal["development", "production", "docker_dev"]

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    eodhd_api_key: str
    
    motherduck_token: str = "" # When blank, lake.py falls back to local DuckDB file (unique_stocks.db)
    s3_bucket: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_region: str = "ap-southeast-2"

    prefect_api_url: str = "http://127.0.0.1:4200/api"
    prefect_api_key: str = ""

    environment: Environment = "development"

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        allowed = {"development", "production"}
        if v not in allowed:
            raise ValueError(f"environment must be one of {allowed}, got {v!r}")
        return v

    @property
    def is_production(self) -> bool:
        return self.environment == "production"

    @property
    def is_development(self) -> bool:
        return self.environment == "development"

    @property
    def duckdb_connection_string(self) -> str:
        if self.motherduck_token:
            return f"md:unique_stocks?motherduck_token={self.motherduck_token}"
        return "unique_stocks.db"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance, loaded on first call."""
    return Settings()  # type: ignore[call-arg]
