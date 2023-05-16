from pydantic import BaseModel, Field
from shared.clients.db.postgres.repositories.base import PgRepositories

# from shared.clients.db.postgres.repositories.mapping_surrogate_key.schema import MappingSurrogateKeyAdd


class MappingSurrogateKeyRepository(PgRepositories):
    table = "mapping_surrogate_key"

    def find_all(self, product: str):
        return self._query.find(
            "SELECT * FROM surrogate_keys WHERE is_active AND product = %(product)s", params={"product": product}
        ).get_polars_df()

    def add(self, data, uid_col_name: str = "uid"):
        class MappingSurrogateKeyAdd(BaseModel):
            product: str
            uid: str = Field(..., alias=uid_col_name)  # type: ignore[literal-required]

        return self._query.add(
            data=data,
            column_model=MappingSurrogateKeyAdd,
            table="surrogate_keys",
            conflict="DO_NOTHING",
            returning="ALL_COLUMNS",
        ).get_polars_df()
