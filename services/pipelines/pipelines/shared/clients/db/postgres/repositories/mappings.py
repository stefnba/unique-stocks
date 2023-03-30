from typing import Optional

from shared.clients.db.postgres.repositories.repository import PgRepositories
from shared.utils.sql.file import QueryFile


class MappingsRepository(PgRepositories):
    def get_mappings(self, source: Optional[str] = None, product: Optional[str] = None, field: Optional[str] = None):
        return self._query.find(
            QueryFile({"base_path": __file__, "path": "./sql/get_mappings.sql"}),
            filters=[
                {"column": "source", "value": source},
                {"column": "product", "value": product},
                {"column": "field", "value": field},
            ],
        ).get_polars_df()
