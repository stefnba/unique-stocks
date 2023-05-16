from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.security_type.schema import SecurityType


class SecurityTypeRepo(PgRepositories):
    table = "security_type"
    schema = SecurityType

    def find_all(self):
        return self._query.find("SELECT * FROM security_type").get_polars_df()
