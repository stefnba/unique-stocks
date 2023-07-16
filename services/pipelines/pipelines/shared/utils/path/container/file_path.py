import uuid
from typing import Literal


from shared.config import CONFIG
from datetime import datetime

from pathlib import Path

DIR_PATH = "/app/downloads/" if not CONFIG.app.env == "Development" else "downloads"


def container_file_path(format: Literal["zip", "json", "csv", "parquet"]) -> str:
    """
    Builds absolute path for temporarily saving a file on local container system.

    Args:
        format: Format of file.

    Returns:
        str: Absolute path.
    """
    time = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    key = uuid.uuid4().hex

    # create dir if not exists
    Path(DIR_PATH).mkdir(parents=True, exist_ok=True)

    return Path(DIR_PATH, f"{time}_{key}.{format.lstrip('.')}").as_posix()
