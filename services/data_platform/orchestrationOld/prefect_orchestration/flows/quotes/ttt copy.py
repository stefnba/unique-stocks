import asyncio

import aiohttp
from prefect import flow, task


@task
async def fetch(session, number):
    pokemon_url = f"https://pokeapi.co/api/v2/pokemon/{number}"
    async with session.get(pokemon_url) as response:
        a = await response.json()
        print(a.get("name"))
        return a


@task
def fetch_all():
    async def _fetch_all():
        async with aiohttp.ClientSession() as session:
            tasks = [fetch(session, url) for url in range(1, 500)]
            results = await asyncio.gather(*tasks)
            return results

    a = asyncio.run(_fetch_all())

    return a


@task
def process_result(result):
    print(result[:100])  # Print the first 100 characters of each result for brevity


@flow(log_prints=True)
def async_test3() -> None:

    results = fetch_all()
    for result in results:
        process_result(result)


if __name__ == "__main__":

    async_test3()
