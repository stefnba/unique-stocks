from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.exhange.schema import Exchange
from shared.utils.sql.file import QueryFile
from typing import Optional


class ExchangeRepository(PgRepositories):
    table = ("data", "exchange")
    schema = Exchange

    def find_all(self, source: Optional[str] = None):
        return self._query.find(
            "SELECT * FROM data.exchange", filters=[{"column": "source", "operator": "EQUAL", "value": source}]
        ).get_polars_df(schema=self.schema)

    def add(self, data):
        return self._query.add(
            data,
            table=self.table,
            column_model=self.schema,
            returning="ALL_COLUMNS",
            conflict={
                "target": ["id"],
                "action": [
                    {"column": "is_active", "value": True},
                    {"column": "updated_at", "value": "now()"},
                    {"column": "active_until", "value": None},
                ],
            },
        ).get_polars_df(schema=self.schema)

    def mic_operating_mic_mapping(self):
        return self._query.find(QueryFile("./sql/mic_operating_mic_mapping.sql")).get_polars_df()
