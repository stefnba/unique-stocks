import asyncio

from prefect import flow, task


@task
async def async_task(n):
    await asyncio.sleep(1)
    return n


@task
async def my_task():
    print("by wor")
    return 233


@task
async def my_task2():
    print("by wor")
    return 233


@flow
async def testterst():
    res = await asyncio.gather(*(async_task(n) for n in range(0, 3, 1)))
    return res


@flow(log_prints=True)
def hello_world(name: str = "world", goodbye: bool = False):
    print(f"Hello {name} from Prefect! 🤗")

    asyncio.run(testterst())

    if goodbye:
        print(f"Goodbye {name}!")
