from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.exhange.schema import Exchange
from shared.utils.conversion import converter
from shared.utils.sql.file import QueryFile


class ExchangeRepository(PgRepositories):
    table = ("data", "exchange")

    def find_all(self):
        return self._query.find(
            "SELECT * FROM exchange",
        ).get_all()

    def add(self, data):
        return self._query.add(
            data,
            table=self.table,
            column_model=Exchange,
            returning="ALL_COLUMNS",
            conflict={
                "target": ["id"],
                "action": [
                    {"column": "is_active", "value": True},
                    {"column": "updated_at", "value": "now()"},
                    {"column": "active_until", "value": None},
                ],
            },
        ).get_polars_df(converter.model_to_polars_schema(Exchange))

    def mic_operating_mic_mapping(self):
        return self._query.find(QueryFile("./sql/mic_operating_mic_mapping.sql")).get_polars_df()
