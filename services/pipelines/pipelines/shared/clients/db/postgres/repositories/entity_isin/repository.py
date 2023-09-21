from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.entity_isin.schema import EntityIsin


class EntityIsinRepo(PgRepositories):
    table = ("data", "entity_isin")
    schema = EntityIsin

    def find_all(self):
        return self._query.find("SELECT * FROM data.entity_isin").get_polars_df(schema=self.schema)

    def bulk_add(self, data):
        add = self._query.bulk_add(
            data=data,
            table=self.table,
            columns=[
                "id",
                "entity_id",
                "isin",
            ],
            returning="ALL_COLUMNS",
            # conflict={
            #     "target": ["id"],
            #     "action": [
            #         {"column": "is_active", "value": True},
            #         {"column": "updated_at", "value": "now()"},
            #         {"column": "active_until", "value": None},
            #     ],
            # },
        )
        return str(add)
