from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.exhange.schema import Exchange
from shared.utils.sql.file import QueryFile


class ExchangeRepository(PgRepositories):
    table = "exchange"

    def find_all(self):
        return self._query.find(
            "SELECT * FROM exchange",
        ).get_all()

    def add(self, data):
        return self._query.add(data, table=self.table, column_model=Exchange, returning="ALL_COLUMNS").get_all(Exchange)

    def mic_operating_mic_mapping(self):
        return self._query.find(QueryFile("./sql/mic_operating_mic_mapping.sql")).get_polars_df()
