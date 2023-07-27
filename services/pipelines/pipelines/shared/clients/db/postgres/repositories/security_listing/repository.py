from shared.clients.db.postgres.repositories.base import PgRepositories
from shared.clients.db.postgres.repositories.security_listing.schema import SecurityListing


class SecurityListingRepo(PgRepositories):
    table = ("data", "security_listing")
    schema = SecurityListing

    def find_all(self, source: str):
        return self._query.find(
            "SELECT security_listing.*, exchange.mic FROM data.security_listing LEFT JOIn data.exchange ON exchange_id = exchange.id WHERE quote_source = %(source)s",
            params={"source": source},
        ).get_polars_df()

    # get_polars_df(schema=SecurityListing)

    def find_all_with_quote_source(self, source: str):
        return self._query.find(
            "SELECT security_listing.id, exchange.mic, security_ticker.ticker FROM data.security_listing LEFT JOIn data.exchange ON exchange_id = exchange.id LEFT JOIN data.security_ticker ON security_ticker_id = security_ticker.id  WHERE quote_source = %(source)s",
            params={"source": source},
        ).get_polars_df()

    def add(self, data):
        return self._query.add(
            data=data,
            column_model=SecurityListing,
            conflict={
                "target": ["id"],
                "action": [
                    {"column": "quote_source", "excluded": "quote_source"},
                    {"column": "currency", "excluded": "currency"},
                    {"column": "updated_at", "value": "now()"},
                ],
            },
            table=self.table,
            returning="ALL_COLUMNS",
        ).get_polars_df(schema=self.schema)

        """
        
        INSERT INTO security_listing (id, figi, quote_source)
		VALUES(135, 'BBG00P1FYJP3', 'EOd') ON CONFLICT (id)
		DO
		UPDATE
		SET
			quote_source = EXCLUDED.quote_source
        """
