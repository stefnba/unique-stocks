from typing import Any, Literal

import duckdb
import pandas as pd
import polars as pl
from shared.utils.path.data_lake.file_path import DataLakeFilePathModel

DataInput = pl.DataFrame | pd.DataFrame | str | duckdb.DuckDBPyRelation | dict[Any, Any] | list
Path = str | DataLakeFilePathModel

DataFormat = Literal["json", "parquet", "csv"]
