from typing import Literal

from pydantic import BaseSettings


class DatalakeConfig(BaseSettings):
    zones = Literal["raw", "processed", "curated", "temp"]
