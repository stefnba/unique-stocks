#!/usr/bin/env python
import sys

from alembic.config import Config
from alembic import command
from pathlib import Path
from datetime import datetime

SQL_FILE_TEMPLATE = "/* UP */\n\n/* DOWN */"


def create_sql_file(revision_name: str, revision_id: str):
    location = alembic_cfg.get_main_option("sql_file_location")
    if not location:
        raise ValueError('Option "sql_file_location" not specified in alembic.ini')

    file_name = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_{revision_id}_{revision_name}.sql"

    path = Path(location, file_name)

    with open(path, "w") as f:
        f.write(SQL_FILE_TEMPLATE)


def create_revision_file(revision_name: str):
    revision = command.revision(alembic_cfg, message=revision_name)
    revision_id = revision.__dict__.get("revision", None)

    if not revision_id:
        raise ValueError("No revision ID was returned")

    create_sql_file(revision_name=revision_name, revision_id=revision_id)


params = sys.argv[1:]
config_path = params.pop(0)

revision_name = params.pop(0)

alembic_cfg = Config(config_path)


create_revision_file(revision_name)
