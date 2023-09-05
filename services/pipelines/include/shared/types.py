from typing import Literal, TypeAlias

"""Data products"""
DataProducts: TypeAlias = Literal[
    "exchange", "security", "security_ticker", "security_listing", "security_quote", "fundamental", "security", "entity"
]
DataSources: TypeAlias = Literal["EodHistoricalData", "OpenFigi"]

"""Data lake"""
DataLakeZone: TypeAlias = Literal["raw", "transformed", "temp", "curated"]
DataLakeDataFileTypes: TypeAlias = Literal["csv", "parquet", "json"]
