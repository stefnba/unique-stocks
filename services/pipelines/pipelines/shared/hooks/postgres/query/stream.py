from shared.hooks.postgres.query.base import QueryBase

import psycopg
from psycopg import sql
import uuid
import os
import polars as pl
from psycopg.rows import dict_row
import pandas as pd
from typing import TypeAlias, Optional, Tuple, Callable, Any
from pydantic import BaseModel
import shutil

from shared.loggers.logger import db as logger
from shared.loggers.events import database as log_events

DataInput: TypeAlias = str | pl.DataFrame | pd.DataFrame


from psycopg import Column, Cursor
from typing import Optional, List


class StreamFrame(BaseModel):
    data: pl.LazyFrame
    length: int
    cleanup: Callable[[], Any]

    class Config:
        arbitrary_types_allowed = True


def convert_pg_to_pl_schema(columns: Optional[List[Column]]):
    mapping = {"int4": pl.Int64, "text": pl.Utf8, "bool": pl.Boolean, "timestamp": pl.Datetime}

    if columns:
        return {c.name: mapping[c._type_display()] for c in columns if c._type_display() in mapping}
    return {}


def create_tmp_dir():
    dir_path = f"downloads/{str(uuid.uuid4())}"

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    return dir_path


def cleanup(dir_path: str):
    """Remove temporary directory with parquet files."""
    shutil.rmtree(dir_path)
    return None


class StreamQuery(QueryBase):
    def stream(self, length=250 * 1000) -> StreamFrame:
        """
        Execute a query using PostgreSQL COPY command.
        A temporary table is created and populated using the COPY command and then records are inserted into the
        final table using the INSERT INTO command to handle conflicts with ON CONFLICT.
        """

        dir_path = create_tmp_dir()

        with psycopg.connect(self._conn_uri, row_factory=dict_row) as conn:
            with psycopg.ServerCursor(
                conn,
                name="cs",
            ) as cur:
                cur.execute("SELECT * FROM data.entity")

                data_length = cur.rowcount

                while True:
                    result = cur.fetchmany(length)

                    if len(result) == 0:
                        break

                    pl.DataFrame(result, schema=convert_pg_to_pl_schema(cur.description)).write_parquet(
                        f"{dir_path}/{str(uuid.uuid4())}.parquet"
                    )

        return StreamFrame(data=pl.scan_parquet(f"{dir_path}/*"), length=data_length, cleanup=lambda: cleanup(dir_path))
