from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.security_listing.schema import SecurityListing


class SecurityListingRepo(PgRepositories):
    table = ("data", "security_listing")
    schema = SecurityListing

    def find_all(self):
        return self._query.find("SELECT * FROM data.security_listing").get_polars_df()

    def add(self, data):
        return self._query.add(
            data=data, column_model=SecurityListing, conflict="DO_NOTHING", table=self.table, returning="ALL_COLUMNS"
        ).get_polars_df(schema=self.schema)

        """
        
        INSERT INTO security_listing (id, figi, quote_source)
		VALUES(135, 'BBG00P1FYJP3', 'EOd') ON CONFLICT (id)
		DO
		UPDATE
		SET
			quote_source = EXCLUDED.quote_source
        """
