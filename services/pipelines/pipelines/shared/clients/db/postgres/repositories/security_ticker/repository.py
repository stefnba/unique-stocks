from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.security_ticker.schema import SecurityTicker


class SecurityTickerRepo(PgRepositories):
    table = ("data", "security_ticker")

    def find_all(self):
        return self._query.find("SELECT * FROM data.security_ticker").get_polars_df()

    def add(self, data):
        return self._query.add(
            data=data, column_model=SecurityTicker, table=self.table, conflict="DO_NOTHING", returning="ALL_COLUMNS"
        ).get_polars_df()
