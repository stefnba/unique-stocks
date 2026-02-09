import asyncio
import typing as t

from aiohttp import ClientSession
from lib.hooks.http.asynchronous import create_async_http_client
from prefect import flow, task
from pydantic import BaseModel


class Security(BaseModel):
    code: str
    exchange_code: str


@task(name="Retrieve Quotes from API")
async def retrieve_quotes_from_api(client: ClientSession, security: Security) -> list[int]:
    url = "https://pokeapi.co/api/v2/pokemon/1"

    async with client.get(url) as resp:
        pokemon = await resp.json()
        print(pokemon["name"])

    return [1, 2, 3, 4, 5]


@task(name="Upload to S3")
async def upload_quotes_to_s3(security: Security, quotes: list[int]):

    return "s3://bucket/quotes"


@flow(
    name="Get Quotes for One Security",
    flow_run_name="{security}",
)
async def get_quotes_for_one_security(client: ClientSession, security: Security) -> str:
    print(f"Getting quotes for security '{security.code}' at exchange '{security.exchange_code}'...")
    quotes = await retrieve_quotes_from_api.submit(client=client, security=security)
    file_path = await upload_quotes_to_s3.submit(security=security, quotes=[1, 2, 3])

    print(
        f"Quotes for security '{security.code}' at exchange '{security.exchange_code}' uploaded to '{file_path.result()}'"
    )

    return "e"


@flow(
    name="Get Quotes",
    description="Get historical quotes for a list of securities from API",
)
async def get_quotes_for_securities(securities: list[Security]):

    s3_path = "s3://bucket/quotes"

    http_client = create_async_http_client()

    async with http_client:
        await asyncio.gather(
            *t.cast(
                t.Iterable[t.Awaitable[str]],
                [get_quotes_for_one_security(client=http_client, security=s) for s in securities],
            )
        )
    return s3_path


@task
def consolidate(res):
    print("consolidate", res)


@flow(log_prints=True)
async def historical_quotes_elt():
    rest = await get_quotes_for_securities(
        [
            Security(code="AAPL", exchange_code="NASDAQ"),
            Security(code="AMZN", exchange_code="NASDAQ"),
        ]
    )

    consolidate(res=rest)

    return rest


if __name__ == "__main__":

    async def run():
        await historical_quotes_elt()

    asyncio.run(run())
