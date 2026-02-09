import asyncio

import aiohttp


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


async def main():
    urls = [
        "https://api.github.com",
        "https://jsonplaceholder.typicode.com/posts",
        "https://jsonplaceholder.typicode.com/comments",
    ]

    async with aiohttp.ClientSession() as session:
        # Create a list of tasks
        tasks = [fetch(session, url) for url in urls]

        # Use asyncio.gather to run all tasks concurrently
        results = await asyncio.gather(*tasks)

        for result in results:
            print(result[:100])  # Print the first 100 characters of each result for brevity


# Run the main coroutine
asyncio.run(main())
