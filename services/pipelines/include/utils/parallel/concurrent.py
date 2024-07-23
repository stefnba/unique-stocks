import time
import typing as t
from concurrent.futures import ThreadPoolExecutor


def proces_paralell(task: t.Callable, iterable: t.Iterable, max_workers=20, *args, **kwargs):
    print("Start execution.")

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        s = time.perf_counter()

        for i in iterable:
            future = executor.submit(task, i, *args, **kwargs)

            if future.exception():
                raise Exception(f"Exception for task with {i}: {future.exception()}")

            else:
                result = future.result()
                results.append(result)

    print(f"Execution time: {(time.perf_counter() - s):.4f} seconds.")

    return results
