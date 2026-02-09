# from flows.test import hello_world
# from prefect import serve

# if __name__ == "__main__":
#     hello_world_deploy = hello_world.to_deployment(name="test")

#     serve(hello_world_deploy)  # type: ignore
import asyncio

from flows.quotes.async_2 import async_test as async_test_2
from flows.quotes.async_3 import async_test_3
from flows.quotes.async_flow_new import async_flow_new
from flows.quotes.async_in_one import async_test_one
from flows.quotes.syn import sync_test
from flows.quotes.ttt import async_test
from prefect import flow, serve, task


@task
async def call_api(n: int) -> int:
    await asyncio.sleep(1)
    print(f"call_api {n}")
    return n


@task
async def upload_to_s3(n: int) -> int:

    await call_api.submit(n)

    return n


@flow(name="Subflow")
async def my_subflow(msg):
    print(f"Subflow says: {msg}")
    res = await asyncio.gather(*(upload_to_s3(n) for n in range(0, 100, 1)))
    print(res)
    return res


@task
def consolidate(res):
    print("consolidate", res)


@flow(log_prints=True)
async def async_flow():
    rest = await my_subflow("Hello, world!")

    consolidate(res=rest)


if __name__ == "__main__":

    # asyncio.run(historical_quotes_elt.serve(name="Historical Quotes ETL"))

    # asyncio.run(async_flow.serve("teslkasdfjlkasdf"))

    t = async_test.to_deployment("async")
    b = sync_test.to_deployment("sync")
    c = async_test_2.to_deployment("async_2")
    d = async_test_one.to_deployment("async_one")
    e = async_test_3.to_deployment("async_3")
    s = async_flow_new.to_deployment("async_flow_new")

    serve(s)
