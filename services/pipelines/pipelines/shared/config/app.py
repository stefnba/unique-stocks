from typing import Literal

from pydantic import BaseModel, BaseSettings, Field
from shared.config.env import EnvBaseConfig
from shared.config.types import EnvironmentTypes


class Exchanges(BaseModel):
    composite_exchanges = [
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


class DataConfig(BaseSettings):
    products = Literal["exchange", "security", "security_ticker", "security_listing", "security_quote"]
    sources = Literal["EodHistoricalData", "OpenFigi"]
    exchanges = Exchanges()


class AppConfig(EnvBaseConfig):
    """
    Application configurations.
    """

    env: EnvironmentTypes = Field(env="ENV", default="Development")
