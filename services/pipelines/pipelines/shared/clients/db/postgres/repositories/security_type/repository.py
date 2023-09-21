from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.security_type.schema import SecurityType


class SecurityTypeRepo(PgRepositories):
    table = ("data", "security_type")
    schema = SecurityType

    def find_all(self):
        return self._query.find("SELECT * FROM data.security_type").get_polars_df()
