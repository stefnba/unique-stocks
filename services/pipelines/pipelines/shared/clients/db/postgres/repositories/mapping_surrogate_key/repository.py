from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.utils.sql.file import QueryFile
import polars as pl


class MappingSurrogateKeyRepository(PgRepositories):
    table = ("mapping", "surrogate_key")

    def find_all(self, product: str):
        return self._query.find(QueryFile("./sql/get.sql"), params={"product": product}).get_polars_lf(
            schema={"uid": pl.Utf8, "surrogate_key": pl.Int64}
        )

    def add(self, data):
        """Add to surrogate_key using COPY statement"""
        return self._query.bulk_add(
            data=data, table=self.table, columns=["uid", "product"], returning="ALL_COLUMNS", conflict="DO_NOTHING"
        )
