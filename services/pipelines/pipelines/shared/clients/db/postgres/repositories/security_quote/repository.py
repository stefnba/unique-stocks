from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.security_quote.schema import SecurityQuote


class SecurityQuoteRepo(PgRepositories):
    table = ("data", "security_quote")

    def find_all(self):
        return self._query.find("SELECT * FROM data.security_quote").get_polars_df()

    def add(self, data):
        return self._query.add(
            data=data, column_model=SecurityQuote, table=self.table, conflict="DO_NOTHING", returning="ALL_COLUMNS"
        ).get_polars_df()
