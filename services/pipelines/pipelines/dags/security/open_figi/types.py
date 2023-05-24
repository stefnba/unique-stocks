from typing import Dict, TypedDict

import polars as pl


class FigiSecurityTypeDict(TypedDict):
    name_figi: str
    name_figi2: str
    market_sector_figi: str


FigiSecurityTypeMappingDict = Dict[int, FigiSecurityTypeDict]


class MappingPrep(TypedDict):
    missing: pl.DataFrame
    existing: pl.DataFrame
