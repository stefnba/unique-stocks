from shared.hooks.postgres.query.base import QueryBase
from shared.hooks.postgres.types import QueryInput
import psycopg
from psycopg import sql
from shared.utils.sql.file import QueryFile
import polars as pl
from typing import TypeAlias, Optional

DataInput: TypeAlias = str | pl.DataFrame


def _classify_data(data: DataInput) -> str:
    if isinstance(data, str):
        return data
    if isinstance(data, pl.DataFrame):
        return str(data.to_pandas().to_csv(index=False))


class CopyQuery(QueryBase):
    def bulk_add(self, data: pl.DataFrame, table: str, schema: Optional[str], columns: list[str]):
        """
        Execute a query using PostgreSQL COPY command.
        """
        with psycopg.connect(self._conn_uri) as conn:
            table_identifier = sql.Identifier(table)

            if schema:
                table_identifier = sql.Identifier(schema, table)

            with conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL(
                        """
                        BEGIN;
                        CREATE TEMP TABLE "tmp_table"(
                            LIKE {table} INCLUDING DEFAULTS
                        ) ON COMMIT DROP;
                        """
                    ).format(table=table_identifier)
                )

                copy_query = sql.SQL(
                    """
                    COPY
                        "tmp_table" ({columns})
                    FROM
                        STDIN(FORMAT csv, HEADER TRUE, DELIMITER ',');
                """
                ).format(columns=sql.SQL(", ").join(sql.Identifier(n) for n in columns))

                # remove columns from DataFrame that are not part of COPY statement
                data = data[columns]

                with cursor.copy(copy_query) as copy:
                    copy.write(_classify_data(data))

                cursor.execute(
                    sql.SQL(
                        """
                    INSERT INTO {table}
                    SELECT *
                    FROM tmp_table
                    ON CONFLICT DO NOTHING;
                    COMMIT;
                    """
                    ).format(table=table_identifier)
                )
