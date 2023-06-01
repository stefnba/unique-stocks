from configparser import ConfigParser
from pathlib import Path
from typing import Optional

ALEMBIC_CONFIG = "database/alembic.ini"


def get_seed_file_path(table: str, schema: Optional[str] = None):
    config = ConfigParser()
    config.read(ALEMBIC_CONFIG)

    config.sections()
    seed_location = config.get("alembic", option="seed_location")

    file_name = f"{table}.csv"
    if schema:
        file_name = f"{schema}__{file_name}"

    return Path(seed_location, file_name)
