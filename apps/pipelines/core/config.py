from functools import lru_cache
from typing import Literal
from prefect.blocks.system import Secret
from pydantic import field_validator, model_validator, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

type Environment = Literal["dev", "prod", "docker_dev"]

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    
    # Data provider
    eodhd_api_key: SecretStr = Field(default=SecretStr(""), description="API key for EODHD.")
    motherduck_token: SecretStr = Field(default=SecretStr(""), description="MotherDuck token. When blank, lake.py falls back to local DuckDB file (unique_stocks.db).")
    
    # Storage provider (S3)
    aws_access_key_id: str = ""
    aws_secret_access_key: SecretStr = Field(default=SecretStr(""), description="Secret access key for AWS.")
    s3_bucket: str | None = None
    aws_region: str = "ap-southeast-2"

    # Prefect
    prefect_api_url: str = "http://127.0.0.1:4200/api"
    prefect_api_key: SecretStr = Field(default=SecretStr(""), description="API key for Prefect.")
    
    # Environment
    environment: Environment = "dev"


    @property
    def is_production(self) -> bool:
        return self.environment == "prod"

    @property
    def duckdb_connection_string(self) -> str:
        if self.motherduck_token:
            return f"md:unique_stocks?motherduck_token={self.motherduck_token}"
        return "unique_stocks.db"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance, loaded on first call."""
    return Settings()  

SETTINGS = get_settings()
