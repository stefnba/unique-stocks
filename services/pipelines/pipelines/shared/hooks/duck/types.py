import pandas as pd
import polars as pl
from duckdb import DuckDBPyRelation
from shared.utils.sql.file import QueryFile
from typing_extensions import LiteralString

QueryInput = LiteralString | QueryFile


BindingsBase = str | bool | int | None
BindingsParams = BindingsBase | pl.DataFrame | pd.DataFrame | list[BindingsBase] | DuckDBPyRelation
