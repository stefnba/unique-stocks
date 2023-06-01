from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.security.schema import Security


class SecurityRepo(PgRepositories):
    table = "data.security"

    def find_all(self):
        return self._query.find("SELECT * FROM data.security").get_polars_df()

    def add(self, data):
        return self._query.add(
            data=data, column_model=Security, table=self.table, conflict="DO_NOTHING", returning="ALL_COLUMNS"
        ).get_polars_df()
