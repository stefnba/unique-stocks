import time
import typing as t
from concurrent.futures import ThreadPoolExecutor


def proces_paralell(task: t.Callable, iterable: t.Iterable, max_workers=20, *args, **kwargs):
    print("Start execution.")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        s = time.perf_counter()
        [executor.submit(task, i, *args, **kwargs) for i in iterable]

    print(f"Execution time: {time.perf_counter() - s} seconds.")
