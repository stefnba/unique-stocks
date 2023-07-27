import polars as pl
from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.mapping_figi.schema import MappingFigi
from shared.utils.sql.file import QueryFile


class MappingFigiRepository(PgRepositories):
    table = ("mapping", "figi")
    schema = MappingFigi

    def find_all(self):
        return self._query.find(QueryFile("./sql/get.sql")).get_polars_df(schema=self.schema)

    def add(self, data) -> None:
        if len(data) == 0:
            return

        self._query.add(
            data=data, column_model=self.schema, table=self.table, conflict="DO_NOTHING", returning="ALL_COLUMNS"
        ).get_all()
