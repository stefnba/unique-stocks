import asyncio

import aiohttp
from prefect import flow, task


@task
def get_exchanges() -> list[str]:
    return ["NYSE", "NASDAQ", "LSE", "HKEX", "TSE", "SSE", "SZSE"]


@task(tags=["fetch"])
async def fetch(number: int) -> str:
    pokemon_url = f"https://pokeapi.co/api/v2/pokemon/{number}"

    async with aiohttp.ClientSession() as session:
        async with session.get(pokemon_url) as response:
            a = await response.json()
            name: str = a.get("name")
            print(name)
            return name


@task(tags=["upload"])
async def upload(name: str):
    print(f"uploading {name}")


@task
async def upload_from_url(number: int):
    res = await fetch.submit(number)
    await upload.submit(await res.result())

    return res


@task
async def fetch_all():

    tasks = [upload_from_url.submit(number) for number in range(1, 100)]
    result = await asyncio.gather(*tasks)

    return result


@flow(log_prints=True)
async def async_test_3() -> None:

    exchanges = get_exchanges()
    for exchange in exchanges:
        print(f"Fetching data for {exchange}")

    await fetch_all.submit()


if __name__ == "__main__":

    async def run():
        await async_test_3()

    asyncio.run(run())
