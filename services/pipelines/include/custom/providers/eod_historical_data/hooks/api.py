from custom.hooks.api.shared import SharedApiHook


class EodHistoricalDataApiHook(SharedApiHook):
    default_conn_name = "eod_historical_data"

    def __init__(
        self,
        http_conn_id: str = default_conn_name,
    ) -> None:
        super().__init__(method="GET", http_conn_id=http_conn_id)

        conn = self.get_connection(self.http_conn_id)
        self._base_params = {"api_token": conn.password}

    def fundamental(self, security_code: str, exchange_code: str):
        """Get fundamental of a security listed at a particular exchange."""
        return self.run(endpoint=f"fundamentals/{security_code}.{exchange_code}")

    def exchange(self):
        """Get list of all exchanges available."""
        return self.run(endpoint="exchanges-list", response_format="raw")

    def exchange_security(self, exchange_code: str):
        """Get list of all securities listed at an exchange."""
        return self.run(endpoint=f"exchange-symbol-list/{exchange_code}", data={"fmt": "json"})

    def historical_quote(self, security_code: str, exchange_code: str):
        """
        Get historical stock prices of a security listed at a particular exchange.

        Return format is csv.
        """
        return self.run(endpoint=f"eod/{security_code}.{exchange_code}", data={"fmt": "json"}, response_format="json")
