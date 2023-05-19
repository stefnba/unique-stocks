from pathlib import Path
from typing import Optional, cast, get_args

from shared.hooks.data_lake.dataset import types


def get_format(path: str, format: Optional[types.DataFormat] = None) -> types.DataFormat:
    """
    Derive file format from file path.

    Args:
        path (str): _description_
        format (types.DataFormat): _description_

    Raises:
        Exception: _description_

    Returns:
        types.DataFormat: _description_
    """
    if format is None:
        _format = Path(path).suffix.replace(".", "")
        if _format not in get_args(types.DataFormat):
            raise Exception(f"File format must be of type {get_args(types.DataFormat)}")
        return cast(types.DataFormat, _format)
    else:
        return format
