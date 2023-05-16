from typing import Literal

from pydantic import BaseSettings, Field
from shared.config.env import EnvBaseConfig
from shared.config.types import EnvironmentTypes


class DataConfig(BaseSettings):
    products = Literal["exchange", "security", "security_ticker", "security_listing", "security_quote"]
    sources = Literal["EodHistoricalData"]


class AppConfig(EnvBaseConfig):
    """
    Application configurations.
    """

    env: EnvironmentTypes = Field(env="ENV", default="Development")
