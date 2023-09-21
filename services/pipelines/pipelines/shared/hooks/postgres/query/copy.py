from shared.hooks.postgres.query.base import QueryBase

import psycopg
from psycopg import sql

import polars as pl
import pandas as pd
from typing import TypeAlias, Optional
from shared.hooks.postgres.types import ConflictParams, ReturningParams
from shared.hooks.postgres.query.utils import build_conflict_query, build_returning_query, build_table_name

from shared.loggers.logger import db as logger
from shared.loggers.events import database as log_events

DataInput: TypeAlias = str | pl.DataFrame | pd.DataFrame


def _classify_data(data: DataInput) -> str:
    if isinstance(data, pl.DataFrame):
        return str(data.to_pandas().to_csv(index=False))
    if isinstance(data, pd.DataFrame):
        return str(data.to_csv(index=False))
    return data


def _temp_table_query(table: str | tuple[str, str]) -> sql.Composed:
    return sql.SQL(
        """
        BEGIN;
        CREATE TEMP TABLE "tmp_table"(
            LIKE {table} INCLUDING DEFAULTS
        ) ON COMMIT DROP;
        """
    ).format(table=build_table_name(table))


def _insert_query(table: str | tuple[str, str]) -> sql.Composed:
    return sql.SQL(
        """
        INSERT INTO
            {table}
        SELECT *
        FROM tmp_table
        """
    ).format(table=build_table_name(table))


def _copy_query(columns: list[str]) -> sql.Composed:
    return sql.SQL(
        """
        COPY
            "tmp_table" ({columns})
        FROM
            STDIN(FORMAT CSV, HEADER TRUE, DELIMITER ',');
    """
    ).format(columns=sql.SQL(", ").join(sql.Identifier(n) for n in columns))


class CopyQuery(QueryBase):
    def bulk_add(
        self,
        data: DataInput,
        table: str | tuple[str, str],
        columns: list[str],
        returning: Optional[ReturningParams] = None,
        conflict: Optional[ConflictParams] = None,
    ):
        """
        Execute a query using PostgreSQL COPY command.
        A temporary table is created and populated using the COPY command and then records are inserted into the
        final table using the INSERT INTO command to handle conflicts with ON CONFLICT.
        """

        logger.info(event=log_events.CopyInit(table=table, columns=columns))

        # remove columns from DataFrame that are not part of COPY statement
        if isinstance(data, pl.DataFrame) or isinstance(data, pd.DataFrame):
            data = data[columns]

        with psycopg.connect(self._conn_uri) as conn:
            with conn.cursor() as cursor:
                cursor.execute(_temp_table_query(table))

                with cursor.copy(_copy_query(columns)) as copy:
                    # https://stackoverflow.com/questions/74209444/import-of-csv-data-into-postgresql-using-psycopg3-results-in-psycopg-errors-inva
                    # write to temp table
                    copy.write(_classify_data(data))

                insert_query = _insert_query(table)

                if conflict:
                    insert_query += build_conflict_query(conflict)

                if returning:
                    insert_query += build_returning_query(returning)

                logger.info(
                    event=log_events.CopyInsertFromTemp(
                        table=table, columns=columns, query=insert_query.as_string(cursor)
                    )
                )

                # execute insert from temp table
                cursor.execute(insert_query)

                row_count = cursor.rowcount
                # records = cursor.fetchall()

                # committ
                cursor.execute("COMMIT;")
                logger.info(event=log_events.CopySuccess(table=table, columns=columns, row_count=row_count))

                return row_count
