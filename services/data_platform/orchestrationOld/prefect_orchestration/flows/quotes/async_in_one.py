import asyncio

import aiohttp
from prefect import flow


async def fetch(session, number):
    pokemon_url = f"https://pokeapi.co/api/v2/pokemon/{number}"

    async with session.get(pokemon_url) as response:
        a = await response.json()
        print(a.get("name"))
        return a


@flow(log_prints=True)
def async_test_one() -> None:

    async def _fetch_all():
        async with aiohttp.ClientSession() as session:
            tasks = [fetch(session, url) for url in range(1, 500)]
            results = await asyncio.gather(*tasks)

            return results

    asyncio.run(_fetch_all())


if __name__ == "__main__":

    async_test_one()
