from pydantic import Field

from .env import EnvBaseConfig


class ApiKeys(EnvBaseConfig):
    eod_historical_data: str = Field(env="EOD_API_KEY", default=None)
