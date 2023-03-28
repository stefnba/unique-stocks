import sys
from string import Template
from typing import Any, Optional

import duckdb
import pandas as pd
import polars as pl
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from fsspec import AbstractFileSystem

# from shared.utils.sql.file import QueryFile
from shared.hooks.duck.types import BindingsParams, QueryInput
from shared.utils.path.datalake.builder import DatalakePathBuilder
from shared.utils.sql.file import QueryFile


class DuckDbHelpers:
    build_abfs_path = DatalakePathBuilder.build_abfs_path
    QueryFile = QueryFile


class DuckDbHook:
    db: DuckDBPyConnection
    options: str
    helpers = DuckDbHelpers

    def __init__(self, options: str = "", file_system: Optional[AbstractFileSystem] = None):
        self.options = options

        self.db = duckdb.connect(":memory:")
        self.db.query(self.options)

        if file_system:
            self.register_file_system(file_system)

    def query(self, query: QueryInput, **bindings: BindingsParams) -> DuckDBPyRelation:
        """
        Run a duckdb SQL query.

        Important: If a path to a .sql file is provided for query arg, it needs to be specified directly and not
        returned by another function as sys._getframe(1).f_globals then picks up the wrong base_path.

        Args:
            statement (QueryInput): Query can be a LiteralString, QueryFile or path to a .sql file.

        Returns:
            DuckDBPyRelation: _description_
        """
        # convert bindings to dataframes and register them with DuckDB
        dataframes = self._collect_dataframes(**bindings)
        for key, df in dataframes.items():
            self.db.register(key, df)

        if isinstance(query, str) and query.endswith(".sql"):
            namespace = sys._getframe(1).f_globals  # caller's globals
            query = QueryFile({"base_path": str(namespace.get("__file__")), "path": query})

        _query = self._query_to_string(query, **bindings)
        print(_query)
        result = self.db.sql(_query)
        return result

    def _collect_dataframes(self, **bindings: BindingsParams) -> dict[str, Any]:
        """
        Collect all of the DataFrames referenced so they can be registered with DuckDB as views. Currently, DuckDB
        only supports pandas.DataFrame and Arrow table, not polars.DataFrame duckdb.DuckDBPyRelation.

        Returns:
            Mapping[str, Any]: _description_
        """

        dfs: dict[str, Any] = {}

        for _, value in bindings.items():
            # pandas df
            if isinstance(value, pd.DataFrame):
                dfs[f"df_{id(value)}"] = value

            # polars df
            if isinstance(value, pl.DataFrame):
                dfs[f"df_{id(value)}"] = value.to_arrow()

            # DuckDB relation
            if isinstance(value, DuckDBPyRelation):
                dfs[f"df_{id(value)}"] = value.to_arrow_table()

        return dfs

    def _query_to_string(self, query: QueryInput, **bindings) -> str:
        """
        Convert QueryInput to string and handle various replacements and substitute template.

        Args:
            query (QueryInput): String or QueryFile

        Returns:
            str: Final query.
        """
        _query = query if isinstance(query, str) else query.sql

        replacements = {}
        for key, value in bindings.items():
            if isinstance(value, (pd.DataFrame, pl.DataFrame, DuckDBPyRelation)):
                replacements[key] = f"df_{id(value)}"
            elif isinstance(value, str):
                replacements[key] = f"'{value}'"
            elif isinstance(value, (int, float, bool)):
                replacements[key] = str(value)
            elif value is None:
                replacements[key] = "NULL"
            else:
                raise ValueError(f"Invalid type for {key}")
        return Template(_query).safe_substitute(replacements)

    def register_file_system(self, file_system: AbstractFileSystem) -> None:
        """
        Register a fsspec compliant filesystem for DuckDB, e.g. fsspec.
        """
        self.db.register_filesystem(file_system)
