"""
https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html
https://medium.com/analytics-vidhya/async-python-client-rate-limiter-911d7982526b
"""


import asyncio
from asyncio.queues import Queue
from asyncio.locks import Semaphore, Lock
from asyncio.tasks import create_task
import aiohttp
import typing as t
import time
import math


class Counter:
    tokens_queue: Queue
    step: int
    total: int

    def __init__(self, step: int = 250) -> None:
        # self.total = len(asyncio.all_tasks(asyncio.get_running_loop()))
        self.tokens_queue = Queue()
        self.step = step

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.tokens_queue.put(1)
        # print(, self.total)

        count = self.tokens_queue.qsize()

        if count % self.step == 0:
            print(f"Status: {count:,} done.")


async def make_async_iterable(iterable):
    for i in iterable:
        yield (i)


class RateLimiter:
    tokens_queue: Queue
    tokens_consumer_task: asyncio.tasks.Task
    rate_limit: float
    semaphore: Semaphore

    def __init__(self, rate_limit: int, period_in_sec=60, concurrency_limit=50):
        if not rate_limit or rate_limit < 1:
            raise ValueError("rate_limit must be non zero positive number")
        if not concurrency_limit or concurrency_limit < 1:
            raise ValueError("concurrent_limit must be non zero positive number")

        self.rate_limit = rate_limit / period_in_sec
        self.tokens_queue = Queue(math.floor(self.rate_limit))

        self.tokens_consumer_task = create_task(self.consume_tokens())
        self.semaphore = Semaphore(concurrency_limit)

    async def add_token(self):
        await self.tokens_queue.put(1)

    async def consume_tokens(self):
        consumption_rate = 1 / self.rate_limit
        last_consumption_time = 0

        while True:
            if self.tokens_queue.empty():
                await asyncio.sleep(consumption_rate)
                continue

            current_consumption_time = time.monotonic()
            total_tokens = self.tokens_queue.qsize()
            tokens_to_consume = self.get_tokens_amount_to_consume(
                consumption_rate, current_consumption_time, last_consumption_time, total_tokens
            )

            for _ in range(0, tokens_to_consume):
                self.tokens_queue.get_nowait()

            last_consumption_time = time.monotonic()

            await asyncio.sleep(consumption_rate)

    @staticmethod
    def get_tokens_amount_to_consume(consumption_rate, current_consumption_time, last_consumption_time, total_tokens):
        time_from_last_consumption = current_consumption_time - last_consumption_time
        calculated_tokens_to_consume = math.floor(time_from_last_consumption / consumption_rate)
        tokens_to_consume = min(total_tokens, calculated_tokens_to_consume)
        return tokens_to_consume

    async def __aenter__(self):
        await self.add_token()
        await self.semaphore.acquire()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.semaphore.release()
        if exc_type:
            # log error here and safely close the class
            pass


class AsyncIoRoutine:
    coros: list[t.Coroutine]
    lock: Lock

    limiter: RateLimiter
    errors: list[Exception]

    def __init__(self, name: t.Optional[str] = None) -> None:
        self.lock = Lock()
        self.coros = []
        self.errors = []

    async def coro_task(self, *args, **kwargs):
        raise NotImplementedError("Method not implemented.")

    async def gather(self):
        result = await asyncio.gather(*self.coros)
        return result

    async def main(self, *args, **kwargs):
        raise NotImplementedError("Method not implemented.")

    def run(self, *args, **kwargs):
        print("Starting execution.")
        s = time.perf_counter()

        result = asyncio.run(self.main(*args, **kwargs))  # type: ignore

        print(f"Execution time: {(time.perf_counter() - s):0.2f} seconds. {len(self.errors)} errors occured.")
        return result

    @staticmethod
    def get_http_client(tcp_connector_limit=50):
        connector = aiohttp.TCPConnector(limit=tcp_connector_limit)
        return aiohttp.ClientSession(raise_for_status=True, trust_env=True, connector=connector)
