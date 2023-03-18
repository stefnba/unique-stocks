from typing import Sequence
from datetime import datetime


def path(*args: str | Sequence[str | None] | None) -> str:
    """
    Joins given arguments into an path or url.
    Slashes are stripped for each argument.
    """
    full_path: list[str] = []

    for arg in args:
        if isinstance(arg, list):
            _path = "/".join(map(lambda x: str(x).strip("/"), [a for a in arg if a is not None]))
            full_path.append(_path)
        if isinstance(arg, str):
            full_path.append(arg)

    return "/".join(full_path)


def file_path(directory: str | Sequence[str | None], filename: str | list[str | None], extension: str) -> str:
    """
    Builds a full file path from parent directory, filename and extension.
    """
    if isinstance(filename, list):
        filename = "_".join([f for f in filename if f is not None])
    filename_with_ext = f'{filename.strip().strip(".")}.{extension.lstrip(".")}'.strip("/")
    return path(directory, filename_with_ext)


def path_with_dateime(date_format: str = "%Y/%m/%d") -> str:
    """
    Helper function to build a path that includes current datetime.

    - Year yyyy: %Y
        - Month mm: %m
        - Day dd: %d

    Args:
        format (str, optional): Format how the data should be returned.
        Defaults to '%Y/%m/%d'.

    Returns:
        str: Current datetime in specified format as string
    """
    now = datetime.now()
    return now.strftime(date_format)
