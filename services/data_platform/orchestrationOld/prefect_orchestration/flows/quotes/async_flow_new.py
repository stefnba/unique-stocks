import aiohttp
from prefect import flow, task


@task
async def fetch_rates(number: int) -> str:
    print(f"Fetching rates for {number}")

    url = f"https://pokeapi.co/api/v2/pokemon/{number}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            name: str = data.get("name")
            return name


@task
async def upload_rates(rates):
    print(f"Uploading rates {rates}")


@task
def get_exchanges() -> list[str]:
    return [
        "NYSE",
        "NASDAQ",
        # "LSE",
        # "HKEX",
        # "TSE",
        # "SSE",
        # "SZSE",
    ]


@task
async def ingest_rates_to_s3(number: int):
    print(f"Ingesting rates to S3 for {number}")

    rates = await fetch_rates.submit(number)

    # await upload_rates(rates)

    return 1


@flow(name="Exchange flow", flow_run_name="'{exchange}'", log_prints=True)
def exchange_flow(exchange: str):
    print(f"Fetching data for '{exchange}'")

    # async def _flow():
    #     names = await asyncio.gather(*[ingest_rates_to_s3.submit(i) for i in range(1, 100)])
    #     return names

    for i in range(1, 100):
        ingest_rates_to_s3.submit(i)

    # asyncio.run(_flow())


@flow(log_prints=True)
def async_flow_new():
    exchanges = get_exchanges()

    for exchange in exchanges:
        exchange_flow(exchange)
