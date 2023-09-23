from typing import Literal, TypeAlias

"""Data products"""
DataProducts: TypeAlias = Literal[
    "exchange",
    "security",
    "security_ticker",
    "security_listing",
    "security_quote",
    "fundamental",
    "security",
    "entity",
    "entity_isin",
    "index_member",
]
DataSources: TypeAlias = Literal["EodHistoricalData", "OpenFigi", "Gleif"]

"""Data lake"""
DataLakeZone: TypeAlias = Literal["raw", "transformed", "temp", "curated"]
DataLakeDatasetFileTypes: TypeAlias = Literal["csv", "parquet", "json"]
DataLakeDataFileTypes: TypeAlias = DataLakeDatasetFileTypes | Literal["zip"]
