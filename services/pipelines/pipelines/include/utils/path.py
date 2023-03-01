from datetime import datetime


def build_path(*args: str | list[str | None] | None) -> str:
    """
    Joins given arguments into an path or url. Slashes are
    stripped for each argument.
    """

    def join(args: list[str | None]) -> str:
        return "/".join(
            map(lambda x: str(x).strip("/"), [a for a in args if a is not None])
        )

    _args = [join(a) if isinstance(a, list) else a for a in args]

    return join([a for a in _args if a is not None])


def build_file_path(
    directory: str | list[str | None], filename: str, extension: str
) -> str:
    """
    Builds a full file path from parent directory, filename and extension.
    """
    filename_with_ext = f'{filename.strip().strip(".")}.{extension.lstrip(".")}'.strip(
        "/"
    )
    return build_path(directory, filename_with_ext)


def path_with_dateime(date_format: str = "%Y/%m/%d") -> str:
    """
    Helper function to build a path with current datetime.

    Args:
        format (str, optional): Format how the data should be returned.
        Defaults to '%Y/%m/%d'.

    Returns:
        str: Current datetime in specified format as string
    """
    now = datetime.now()
    return now.strftime(date_format)
