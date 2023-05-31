from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.entity.schema import Entity
from shared.utils.conversion.converter import model_to_polars_schema


class EntityRepo(PgRepositories):
    table = "entity"
    schema = Entity

    def find_all(self):
        return self._query.find("SELECT * FROM entity").get_polars_df()

    def _add(self, data):
        """
        Add helper method.
        """
        return self._query.add(
            data=data, column_model=Entity, table=self.table, conflict="DO_NOTHING", returning="ALL_COLUMNS"
        ).get_polars_df(model_to_polars_schema(self.schema))

    def add(self, data: list[dict]):
        BATCH_SIZE = 50 * 1000

        if isinstance(data, list) and len(data) > BATCH_SIZE:
            print("spliiting")
            batches = [data[i : i + BATCH_SIZE] for i in range(0, len(data), BATCH_SIZE)]

            for batch in batches:
                print(len(batch))
                self._add(batch)

        return self._add(data)
