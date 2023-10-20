from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DataProductSettings(BaseSettings):
    composite_exchanges: list[str] = [
        "US",
        "EU",
        "EO",
        "B2",
        "JP",
        "CN",
        "UH",
        "VN",
        "IN",
        "EY",
        "NO  ",
        "CI",
        "BC",
        "ZS",
        "VC",
        "JY",
        "AI",
        "NX",
        "ED",
        "PA",
        "LI",
        "MSFT UT Equity",
        "LR",
        "CH",
        "CZ",
    ]  # Bloomberg/OpenFigi code


class AppSettings(BaseSettings):
    EnvironmentTypes: Literal["Production", "Development", "DockerDevelopment"] = "Development"
    temp_dir_path: str = Field(alias="TEMP_DIR_PATH", default="temp")


class LoggingSettings(BaseSettings):
    host: str = Field(alias="LOGGING_REMOTE_HOST", default="http://localhost")
    port: int = Field(alias="LOGGING_REMOTE_PORT", default=8112)
    endpoint: str = Field(alias="LOGGING_REMOTE_ENDPOINT", default="/log/add")


class ConfigSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app: AppSettings = AppSettings()
    logging: LoggingSettings = LoggingSettings()
    data_product: DataProductSettings = DataProductSettings()
