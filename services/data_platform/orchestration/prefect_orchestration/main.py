from flows.arte import my_flow
from flows.base_async import async_flow
from flows.etl.exchange.exchange import exchange_flow
from flows.historical import historical_quotes
from flows.progress import etl
from flows.test import test_flow
from prefect import serve

if __name__ == "__main__":

    serve(
        async_flow.to_deployment("async_flow"),
        test_flow.to_deployment("test1"),
        historical_quotes.to_deployment("historical_quotes"),
        my_flow.to_deployment("arte"),
        etl.to_deployment("progress"),
        exchange_flow.to_deployment("exchange"),
    )
