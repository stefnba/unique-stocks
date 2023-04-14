from typing import Literal

from pydantic import BaseSettings


class DataLakeConfig(BaseSettings):
    zones = Literal["raw", "processed", "curated", "temp", "modeled"]
