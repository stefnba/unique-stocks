from pydantic import Field
from shared.config.env import EnvBaseConfig


class ApiKeys(EnvBaseConfig):
    eod_historical_data: str = Field(env="EOD_API_KEY", default=None)
    market_stack: str = Field(env="MARKETSTACK_API_KEY", default=None)
