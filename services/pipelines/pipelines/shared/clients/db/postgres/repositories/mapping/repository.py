from typing import Optional

from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.utils.sql.file import QueryFile
from shared.clients.db.postgres.repositories.mapping.schema import Mapping


class MappingsRepository(PgRepositories):
    table = "mapping.mapping"
    schema = Mapping

    def get_mappings(self, source: Optional[str] = None, product: Optional[str] = None, field: Optional[str] = None):
        return self._query.find(
            QueryFile({"base_path": __file__, "path": "./sql/get_mappings.sql"}),
            filters=[
                {"column": "source", "value": source},
                {"column": "product", "value": product},
                {"column": "field", "value": field},
            ],
        ).get_polars_df()

    def _add(self, data):
        return self._query.add(data=data, column_model=self.schema, table=self.table)

    def add(self, data):
        BATCH_SIZE = 100 * 1000

        if isinstance(data, list) and len(data) > BATCH_SIZE:
            batches = [data[i : i + BATCH_SIZE] for i in range(0, len(data), BATCH_SIZE)]

            for batch in batches:
                print(len(batch))
                self._add(batch)

        return self._add(data)
