from typing import Any, Callable, Optional, Union, overload

import polars as pl
from shared.hooks.data_lake.dataset import hook as dataset_hook

# @overload
# def checkout(checkout_path: str, *, func: Callable[[pl.DataFrame], pl.DataFrame], commit_path: str) -> str:
#     ...


# @overload
# def checkout(checkout_path=None, *, func: Callable[..., pl.DataFrame], commit_path: str) -> str:
#     ...


# @overload
# def checkout(checkout_path: str, *, func: Callable[[pl.DataFrame], pl.DataFrame]) -> pl.DataFrame:
#     ...


# @overload
# def checkout(checkout_path=None, *, func: Callable[..., pl.DataFrame]) -> pl.DataFrame:
#     ...


@overload
def checkout(checkout_path: Optional[str] = None, *, func: Callable[..., pl.DataFrame]) -> pl.DataFrame:
    ...


@overload
def checkout(checkout_path: Optional[str] = None, *, func: Callable[..., pl.DataFrame], commit_path: str) -> str:
    ...


def checkout(
    checkout_path: Optional[str] = None,
    *,
    func: Callable[..., pl.DataFrame],
    commit_path: Optional[str] = None,
) -> str | pl.DataFrame:
    """
    Hook that combines three actions in one function:
    - Downloading a dataset from the Data Lake
    - Execution a function (must return a Polars DataFrame)
    - Uploading the resulting dataset to the Data Lake

    If no commit_path is specified, the resulting dataset as a Polars DataFrame is returned.


    Args:
        checkout_path: Path in the Data Lake for the file to be downloaded.
        func: The (transformation) function to be executed - must return a Polars DataFrame.
        commit_path: Upload path for the resulting dataset.
    """
    if checkout_path:
        data = dataset_hook.download(checkout_path).to_polars_df()
        result = func(data)

    else:
        result = func()

    if commit_path:
        return dataset_hook.upload(result, commit_path)
    else:
        return result
