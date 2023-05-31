from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.fundamental.schema import Fundamental


class FundamentalRepo(PgRepositories):
    table = "fundamental"

    def find_all(self):
        return self._query.find("SELECT * FROM fundamental").get_polars_df()

    def add(self, data):
        return self._query.add(
            data=data, column_model=Fundamental, table=self.table, conflict="DO_NOTHING", returning="ALL_COLUMNS"
        ).get_polars_df()
