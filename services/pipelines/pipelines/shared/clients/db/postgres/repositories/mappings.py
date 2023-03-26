from typing import Optional

from shared.clients.db.postgres.repositories.repository import PgRepositories


class MappingsRepository(PgRepositories):
    def get_mappings(self, source: Optional[str] = None, product: Optional[str] = None):
        return self._query.find(
            "SELECT * FROM mappings",
            filters=[{"column": "source", "value": source}, {"column": "product", "value": product}],
        ).get_polars_df()
