import polars as pl
from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.mapping_figi.schema import MappingFigi


class MappingFigiRepository(PgRepositories):
    table = "mapping_figi"

    def find_all(self):
        return self._query.find("SELECT * FROM mapping_figi").get_polars_df(
            schema={
                "isin": pl.Utf8,
                "wkn": pl.Utf8,
                "ticker": pl.Utf8,
                "ticker_figi": pl.Utf8,
                "exchange_mic": pl.Utf8,
                "exchange_operating_mic": pl.Utf8,
                "exchange_code_figi": pl.Utf8,
                "currency": pl.Utf8,
                "country": pl.Utf8,
                "figi": pl.Utf8,
                "share_class_figi": pl.Utf8,
                "composite_figi": pl.Utf8,
                "security_type_id": pl.Int64,
            }
        )

    def add(self, data):
        return self._query.add(
            data=data, column_model=MappingFigi, table=self.table, conflict="DO_NOTHING", returning="ALL_COLUMNS"
        ).get_all()
